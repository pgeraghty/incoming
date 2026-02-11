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

    ranch_opts = %{max_connections: max_connections, num_acceptors: num_acceptors}

    ranch_opts =
      if tls == :implicit do
        ssl_opts =
          tls_opts
          |> Keyword.take([:certfile, :keyfile, :cacertfile, :versions, :ciphers])

        Map.put(ranch_opts, :socket_opts, ssl_opts)
      else
        ranch_opts
      end

    opts =
      [
        {:domain, to_charlist(domain)},
        {:port, port},
        {:sessionoptions, session_opts},
        {:ranch_opts, ranch_opts}
      ]
      |> maybe_add_protocol(tls)

    :gen_smtp_server.child_spec(name, Incoming.Session, opts)
  end

  defp smtp_domain do
    Application.get_env(:incoming, :domain, "localhost")
  end

  defp validate_tls!(:disabled, _opts), do: :ok

  defp validate_tls!(mode, opts) when mode in [:optional, :required, :implicit] do
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

        validate_key_matches_cert!(certfile, keyfile)

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

  defp validate_key_matches_cert!(certfile, keyfile) do
    {:ok, cert_pem} = File.read(certfile)
    {:ok, key_pem} = File.read(keyfile)

    [{:Certificate, cert_der, _} | _] = :public_key.pem_decode(cert_pem)
    [key_entry | _] = :public_key.pem_decode(key_pem)

    {:OTPCertificate, tbs, _sigalg, _sig} = :public_key.pkix_decode_cert(cert_der, :otp)
    spki = elem(tbs, 7)

    case {elem(spki, 2), :public_key.pem_entry_decode(key_entry)} do
      {{:RSAPublicKey, cert_n, _cert_e},
       {:RSAPrivateKey, _ver, key_n, _key_e, _d, _p, _q, _dp, _dq, _qi, _other}} ->
        if cert_n != key_n do
          raise ArgumentError, "keyfile does not match certfile public key"
        end

        :ok

      _ ->
        # Non-RSA key/cert pairs are allowed but not validated for match here.
        :ok
    end
  end

  defp maybe_add_protocol(opts, :implicit), do: [{:protocol, :ssl} | opts]
  defp maybe_add_protocol(opts, _mode), do: opts

  defp maybe_add_tls_options(opts, _tls_opts, :disabled), do: opts
  defp maybe_add_tls_options(opts, _tls_opts, :implicit), do: opts
  defp maybe_add_tls_options(opts, tls_opts, _mode), do: Keyword.put(opts, :tls_options, tls_opts)
end
