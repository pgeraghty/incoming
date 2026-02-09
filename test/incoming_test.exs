defmodule IncomingTest do
  use IncomingCase, async: false

  test "queues message to disk", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, id} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert File.exists?(Path.join([tmp, "committed", id, "raw.eml"]))
    assert File.exists?(Path.join([tmp, "committed", id, "meta.json"]))
  end
end
