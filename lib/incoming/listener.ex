defmodule Incoming.Listener do
  @moduledoc false

  def child_spec(%{name: _name} = listener) do
    name = Map.fetch!(listener, :name)
    port = Map.get(listener, :port, 2525)
    domain = Map.get(listener, :domain, smtp_domain())
    max_connections = Map.get(listener, :max_connections, 1_000)
    num_acceptors = Map.get(listener, :num_acceptors, 10)
    tls = Map.get(listener, :tls, :disabled)
    tls_opts = Map.get(listener, :tls_opts, [])

    callback_opts = [
      queue: Incoming.Config.queue_module(),
      queue_opts: Incoming.Config.queue_opts(),
      max_message_size: Keyword.get(Incoming.Config.session_opts(), :max_message_size),
      max_recipients: Keyword.get(Incoming.Config.session_opts(), :max_recipients),
      tls_mode: tls,
      tls_opts: tls_opts
    ]

    session_opts =
      [
        callbackoptions: callback_opts
      ]
      |> maybe_add_tls_options(tls_opts)

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

  defp maybe_add_tls_options(opts, []), do: opts
  defp maybe_add_tls_options(opts, tls_opts), do: Keyword.put(opts, :tls_options, tls_opts)
end
