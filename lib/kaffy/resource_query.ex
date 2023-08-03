defmodule Kaffy.ResourceQuery do
  @moduledoc false

  import Ecto.Query

  def list_resource(conn, resource, params \\ %{}) do
    per_page = Map.get(params, "limit", "100") |> String.to_integer()
    page = Map.get(params, "page", "1") |> String.to_integer()
    search = Map.get(params, "search", "") |> String.trim()
    search_fields = Kaffy.ResourceAdmin.search_fields(resource)
    filtered_fields = get_filter_fields(conn.query_params, resource)
    ordering = get_ordering(resource, conn.query_params)

    current_offset = (page - 1) * per_page
    schema = resource[:schema]

    {all, paged} =
      build_query(
        schema,
        search_fields,
        filtered_fields,
        search,
        per_page,
        ordering,
        current_offset
      )

    {current_page, opts} =
      case Kaffy.ResourceAdmin.custom_index_query(conn, resource, paged) do
        {custom_query, opts} ->
          {Kaffy.Utils.repo().all(custom_query, opts), opts}

        custom_query ->
          {Kaffy.Utils.repo().all(custom_query), []}
      end

    do_cache = if search == "" and Enum.empty?(filtered_fields), do: true, else: false
    all_count = cached_total_count(schema, do_cache, all, opts)
    {all_count, current_page}
  end

  def get_ordering(resource, params) do
    default_ordering = Kaffy.ResourceAdmin.ordering(resource)
    default_order_field = Map.get(params, "_of", "nil") |> String.to_existing_atom()
    default_order_way = Map.get(params, "_ow", "nil") |> String.to_existing_atom()

    case is_nil(default_order_field) or is_nil(default_order_way) do
      true -> default_ordering
      false -> [{default_order_way, default_order_field}]
    end
  end

  def fetch_resource(conn, resource, id) do
    schema = resource[:schema]
    query = from(s in schema, where: s.id == ^id)

    case Kaffy.ResourceAdmin.custom_show_query(conn, resource, query) do
      {custom_query, opts} -> Kaffy.Utils.repo().one(custom_query, opts)
      custom_query -> Kaffy.Utils.repo().one(custom_query)
    end
  end

  def fetch_list(_, [""]), do: []

  def fetch_list(resource, ids) do
    schema = resource[:schema]

    from(s in schema, where: s.id in ^ids)
    |> Kaffy.Utils.repo().all()
  end

  def total_count(schema, do_cache, query, opts \\ [])

  def total_count(schema, do_cache, query, opts) do
    result =
      from(s in query, select: fragment("count(*)"))
      |> Kaffy.Utils.repo().one(opts)

    if do_cache and result > 100_000 do
      Kaffy.Cache.Client.add_cache(schema, "count", result, 600)
    end

    result
  end

  def cached_total_count(schema, do_cache, query, opts \\ [])

  def cached_total_count(schema, false, query, opts), do: total_count(schema, false, query, opts)

  def cached_total_count(schema, do_cache, query, opts) do
    Kaffy.Cache.Client.get_cache(schema, "count") || total_count(schema, do_cache, query, opts)
  end

  defp get_filter_fields(params, resource) do
    schema_fields =
      Kaffy.ResourceSchema.fields(resource[:schema]) |> Enum.map(fn {k, _} -> to_string(k) end)

    filtered_fields = Enum.filter(params, fn {k, v} -> k in schema_fields and v != "" end)

    Enum.map(filtered_fields, fn {name, value} ->
      %{name: name, value: value}
    end)
  end

  defp build_query(
         schema,
         search_fields,
         filtered_fields,
         search,
         per_page,
         ordering,
         current_offset
       ) do
    query = from(s in schema)

    query =
      cond do
        is_nil(search_fields) || Enum.empty?(search_fields) || search == "" ->
          query

        true ->
          term = search

          search_term_type = typeof(term)

          search_fields =
            search_fields
            |> filter_unnasociated_fields(schema, search_term_type)

          Enum.reduce(search_fields, query, fn
            {association, fields}, q ->
              fields =
                fields
                |> filter_associated_fields(schema, association, search_term_type)

              query = from(s in q, join: a in assoc(s, ^association))

              Enum.reduce(fields, query, fn f, current_query ->
                from([..., r] in current_query,
                  or_where: field(r, ^f) == ^term
                )
              end)

            f, q ->
              from(s in q, or_where: field(s, ^f) == ^term)
          end)
      end

    query = build_filtered_fields_query(query, filtered_fields)

    limited_query =
      from(s in query, limit: ^per_page, offset: ^current_offset, order_by: ^ordering)

    {query, limited_query}
  end

  defp typeof(value) do
    cond do
      is_binary_id?(value) -> :binary_id
      is_id?(value) -> :id
      true -> :string
    end
  end

  def is_binary_id?(str) when is_binary(str) do
    str |> String.match?(~r/\A[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\z/i)
  end

  def is_binary_id?(_), do: false

  def is_id?(str) when is_binary(str) do
    case Integer.parse(str) do
      {int, ""} when int > 0 -> true
      _ -> false
    end
  end

  def is_id?(_), do: false

  def is_association?(search_field) when is_atom(search_field) do
    false
  end

  def is_association?(_), do: true

  defp filter_unnasociated_fields(search_fields, schema, search_term_type) do
    Enum.filter(search_fields, fn search_field ->
      case is_association?(search_field) do
        false ->
          field_type = Kaffy.ResourceSchema.field_type(schema, search_field)
          field_type == search_term_type

        _ ->
          true
      end
    end)
  end

  defp filter_associated_fields(fields, schema, association, search_term_type) do
    Enum.filter(fields, fn field ->
      association_schema =
        Kaffy.ResourceSchema.association(schema, association).related

      field_type = Kaffy.ResourceSchema.field_type(association_schema, field)
      field_type == search_term_type
    end)
  end

  defp build_filtered_fields_query(query, []), do: query

  defp build_filtered_fields_query(query, [filter | rest]) do
    query =
      case filter.value == "" do
        true ->
          query

        false ->
          field_name = String.to_existing_atom(filter.name)
          from(s in query, where: field(s, ^field_name) == ^filter.value)
      end

    build_filtered_fields_query(query, rest)
  end
end
