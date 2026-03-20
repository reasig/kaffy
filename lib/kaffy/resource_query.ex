defmodule Kaffy.ResourceQuery do
  @moduledoc false

  import Ecto.Query

  @count_threshold 100_000

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

    id_filter = Kaffy.ResourceAdmin.deserialize_id(resource, id)
    query = from(s in schema, where: ^id_filter)

    case Kaffy.ResourceAdmin.custom_show_query(conn, resource, query) do
      {custom_query, after_fetch: after_fetch} when is_function(after_fetch) ->
        entity = Kaffy.Utils.repo().one(custom_query)
        after_fetch.(entity)
      {custom_query, opts} -> Kaffy.Utils.repo().one(custom_query, opts)
      custom_query -> Kaffy.Utils.repo().one(custom_query)
    end
  end

  def fetch_list(_, [""]), do: []

  def fetch_list(resource, ids) do
    schema = resource[:schema]

    primary_keys = Kaffy.ResourceSchema.primary_keys(schema)
    ids = Enum.map(ids, &Kaffy.ResourceAdmin.deserialize_id(resource, &1))

    case build_list_query(schema, primary_keys, ids) do
      {:error, error_msg} -> {:error, error_msg}
      query -> Kaffy.Utils.repo().all(query)
    end
  end

  def total_count(schema, do_cache, query, opts \\ [])

  def total_count(schema, do_cache, query, opts) do
    result =
      if do_cache and Kaffy.Utils.repo().__adapter__() == Ecto.Adapters.Postgres do
        result = estimate_total_count(schema, opts)

        if result > @count_threshold do
          result
        else
          total_count_all(query, opts)
        end
      else
        total_count_all(query, opts)
      end

    if do_cache and result > @count_threshold do
      Kaffy.Cache.Client.add_cache(schema, "count", result, 600)
    end

    result
  end

  defp total_count_all(query, opts) do
    from(s in query, select: fragment("count(*)"))
    |> Kaffy.Utils.repo().one(opts)
  end

  @doc """
  Return the estimated number of rows for a schema.

  This allows us to paginate large tables quickly without risk of timeout.

  https://wiki.postgresql.org/wiki/Count_estimate
  https://wiki.postgresql.org/wiki/Slow_Counting
  """
  def estimate_total_count(schema, opts \\ []) do
    Kaffy.Utils.repo().query(
      "SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = $1",
      [schema.__schema__(:source)],
      opts
    )
    |> case do
      {:ok, %{rows: [[count]]}} ->
        count

      err ->
        raise "Failed to estimate count for #{schema}: #{inspect(err)}"
    end
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
      f = String.to_existing_atom(name)
      field_type = Kaffy.ResourceSchema.field_type(resource[:schema], f)
      %{name: name, value: value, type: field_type}
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
    cond do
      is_nil(search_fields) || Enum.empty?(search_fields) || search == "" ->
        query =
          from(s in schema)
          |> build_filtered_fields_query(filtered_fields)

        limited_query =
          from(s in query, limit: ^per_page, offset: ^current_offset, order_by: ^ordering)

        {query, limited_query}

      true ->
        term =
          search
          |> String.trim()
          |> String.replace("%", "\%")
          |> String.replace("_", "\_")

        search_term_type = typeof(term)

        {fragment_fields, standard_fields} =
          Enum.split_with(search_fields, &is_fragment_field?/1)

        search_fields =
          standard_fields
          |> filter_unnasociated_fields(schema, search_term_type)

        [pk | _] = Kaffy.ResourceSchema.primary_keys(schema)

        base_query = from(s in schema, where: 1 == 2, select: field(s, ^pk))

        search_query =
          Enum.reduce(search_fields, base_query, fn
            {association, fields}, q ->
              fields =
                fields
                |> filter_associated_fields(schema, association, search_term_type)

              Enum.reduce(fields, q, fn f, current_query ->
                other_query = from(s in schema, join: a in assoc(s, ^association), where: field(a, ^f) == ^term, select: field(s, ^pk))
                union(current_query, ^other_query)
              end)

            f, q ->
              other_query = from(s in schema, where: field(s, ^f) == ^term, select: field(s, ^pk))
              union(q, ^other_query)
          end)

        search_query =
          Enum.reduce(fragment_fields, search_query, fn {:json, field_name, json_key}, q ->
            other_query = from(s in schema, where: fragment("?->>? = ?", field(s, ^field_name), ^json_key, ^term), select: field(s, ^pk))
            union(q, ^other_query)
          end)

        query =
          from(s in schema, where: field(s, ^pk) in subquery(search_query))
          |> build_filtered_fields_query(filtered_fields)

        limited_query =
          from(s in query, limit: ^per_page, offset: ^current_offset, order_by: ^ordering)

        {query, limited_query}
    end
  end

  defp is_fragment_field?({:json, _, _}), do: true
  defp is_fragment_field?(_), do: false

  defp typeof(value) do
    cond do
      is_binary_id?(value) -> :binary_id
      true -> :string
    end
  end

  def is_binary_id?(str) when is_binary(str) do
    str |> String.match?(~r/\A[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\z/i)
  end

  def is_binary_id?(_), do: false

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

  defp build_list_query(_schema, [], _key_pairs) do
    {:error, "No private keys. List action not supported."}
  end

  defp build_list_query(schema, [primary_key], ids) do
    ids = Enum.map(ids, fn [{_key, id}] -> id end)
    from(s in schema, where: field(s, ^primary_key) in ^ids)
  end

  defp build_list_query(schema, _composite_key, key_pairs) do
    Enum.reduce(key_pairs, schema, fn pair, query_acc ->
      from(query_acc, or_where: ^pair)
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

          case filter.type do
            {:array, :string} ->
              from(s in query, where: ^filter.value in field(s, ^field_name))

            _ ->
              from(s in query, where: field(s, ^field_name) == ^filter.value)
          end
      end

    build_filtered_fields_query(query, rest)
  end
end
