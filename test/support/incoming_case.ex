defmodule IncomingCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  setup_all do
    tmp = Path.join(System.tmp_dir!(), "incoming_test")
    {:ok, _} = Application.ensure_all_started(:gen_smtp)
    {:ok, tmp: tmp}
  end

  setup %{tmp: tmp} do
    File.rm_rf!(tmp)
    File.mkdir_p!(tmp)
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    Application.put_env(:incoming, :policies, [])
    Application.put_env(:incoming, :delivery, nil)

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100
    )

    restart_app()
    on_exit(fn -> Application.stop(:incoming) end)
    :ok
  end

  defp restart_app do
    Application.stop(:incoming)
    Application.ensure_all_started(:incoming)
  end
end
