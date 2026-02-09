defmodule Incoming.Listener do
  @moduledoc false

  def child_spec(%{name: name} = listener) do
    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [listener]}
    }
  end

  def start_link(listener) do
    name = Map.fetch!(listener, :name)
    port = Map.get(listener, :port, 2525)
    domain = Map.get(listener, :domain, smtp_domain())

    session_opts = [
      callbackoptions: [
        queue: Incoming.Config.queue_module(),
        queue_opts: Incoming.Config.queue_opts()
      ]
    ]

    opts = [
      {:domain, to_charlist(domain)},
      {:port, port},
      {:sessionoptions, session_opts}
    ]

    :gen_smtp_server.start(name, Incoming.Session, opts)
  end

  defp smtp_domain do
    Application.get_env(:incoming, :domain, "localhost")
  end
end
