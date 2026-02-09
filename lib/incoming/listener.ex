defmodule Incoming.Listener do
  @moduledoc false

  def child_spec(%{name: _name} = listener) do
    name = Map.fetch!(listener, :name)
    port = Map.get(listener, :port, 2525)
    domain = Map.get(listener, :domain, smtp_domain())
    max_connections = Map.get(listener, :max_connections, 1_000)
    num_acceptors = Map.get(listener, :num_acceptors, 10)

    session_opts = [
      callbackoptions: [
        queue: Incoming.Config.queue_module(),
        queue_opts: Incoming.Config.queue_opts(),
        max_message_size: Keyword.get(Incoming.Config.session_opts(), :max_message_size),
        max_recipients: Keyword.get(Incoming.Config.session_opts(), :max_recipients)
      ]
    ]

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
end
