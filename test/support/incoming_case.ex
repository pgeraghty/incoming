defmodule IncomingCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  setup_all do
    tmp = Path.join(System.tmp_dir!(), "incoming_test")
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    {:ok, _} = Application.ensure_all_started(:gen_smtp)

    started =
      Application.started_applications()
      |> Enum.any?(fn {app, _, _} -> app == :incoming end)

    if started, do: Application.stop(:incoming)
    {:ok, _} = Application.ensure_all_started(:incoming)

    on_exit(fn -> File.rm_rf(tmp) end)
    on_exit(fn -> Application.stop(:incoming) end)
    {:ok, tmp: tmp}
  end

  setup %{tmp: tmp} do
    File.rm_rf!(tmp)
    File.mkdir_p!(tmp)
    :ok
  end
end
