defmodule Incoming.Listener do
  @moduledoc false

  def child_spec(%{name: _name} = listener) do
    listener = Incoming.Validate.listener!(listener)
    name = Map.fetch!(listener, :name)
    port = Map.get(listener, :port, 2525)
    domain = Map.get(listener, :domain, smtp_domain())
    max_connections = Map.get(listener, :max_connections, 1_000)
    num_acceptors = Map.get(listener, :num_acceptors, 10)
    tls = Map.get(listener, :tls, :disabled)
    tls_opts = Map.get(listener, :tls_opts, [])

    validate_tls!(tls, tls_opts)

    callback_opts = [
      queue: Incoming.Config.queue_module(),
      queue_opts: Incoming.Config.queue_opts(),
      max_message_size: Keyword.get(Incoming.Config.session_opts(), :max_message_size),
      max_recipients: Keyword.get(Incoming.Config.session_opts(), :max_recipients),
      tls_mode: tls,
      tls_opts: tls_opts
    ]

    session_opts =
      [callbackoptions: callback_opts]
      |> maybe_add_tls_options(tls_opts, tls)

    opts = [
      {:domain, to_charlist(domain)},
      {:port, port},
      {:sessionoptions, session_opts},
      {:ranch_opts, %{max_connections: max_connections, num_acceptors: num_acceptors}}
    ]

    :gen_smtp_server.child_spec(name, Incoming.Session, opts)
  end

  defp smtp_domain do
    Application.get_env(:incoming, :domain, "localhost")
  end

  defp validate_tls!(:disabled, _opts), do: :ok

  defp validate_tls!(mode, opts) when mode in [:optional, :required] do
    certfile = Keyword.get(opts, :certfile)
    keyfile = Keyword.get(opts, :keyfile)

    cond do
      is_nil(certfile) or is_nil(keyfile) ->
        raise ArgumentError, "tls_opts must include :certfile and :keyfile when tls is enabled"

      not File.exists?(certfile) ->
        raise ArgumentError, "certfile does not exist: #{certfile}"

      not File.exists?(keyfile) ->
        raise ArgumentError, "keyfile does not exist: #{keyfile}"

      true ->
        validate_pem!(certfile, [:Certificate], "certfile")

        validate_pem!(
          keyfile,
          [:RSAPrivateKey, :DSAPrivateKey, :ECPrivateKey, :PrivateKeyInfo],
          "keyfile"
        )

        :ok
    end
  end

  defp validate_tls!(mode, _opts), do: raise(ArgumentError, "invalid tls mode: #{inspect(mode)}")

  defp validate_pem!(path, types, label) do
    pem = File.read!(path)
    entries = :public_key.pem_decode(pem)

    if entries == [] do
      raise ArgumentError, "#{label} is not valid PEM: #{path}"
    end

    if not Enum.any?(entries, fn {type, _data, _headers} -> type in types end) do
      raise ArgumentError, "#{label} PEM did not contain expected entries: #{path}"
    end

    :ok
  end

  defp maybe_add_tls_options(opts, _tls_opts, :disabled), do: opts
  defp maybe_add_tls_options(opts, tls_opts, _mode), do: Keyword.put(opts, :tls_options, tls_opts)
end
