defmodule IncomingCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  setup do
    tmp = Path.join(System.tmp_dir!(), "incoming_test")
    File.rm_rf!(tmp)
    File.mkdir_p!(tmp)
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    on_exit(fn -> File.rm_rf!(tmp) end)
    {:ok, tmp: tmp}
  end
end
