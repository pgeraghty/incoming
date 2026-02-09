defmodule IncomingTest do
  use IncomingCase, async: false

  test "queues message to disk", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert File.exists?(Path.join([tmp, "committed", message.id, "raw.eml"]))
    assert File.exists?(Path.join([tmp, "committed", message.id, "meta.json"]))
  end

  test "accepts smtp session and queues message", %{tmp: tmp} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)

    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "DATA")
    assert_recv(socket, "354")

    :ok = :gen_tcp.send(socket, "Subject: Test\r\n\r\nBody\r\n.\r\n")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    committed = Path.join(tmp, "committed")
    assert File.exists?(committed)
    assert length(File.ls!(committed)) == 1
  end

  defp send_line(socket, line) do
    :ok = :gen_tcp.send(socket, line <> "\r\n")
  end

  defp assert_recv(socket, prefix) do
    assert {:ok, line} = :gen_tcp.recv(socket, 0, 1000)
    assert String.starts_with?(line, prefix)
  end

  defp read_multiline(socket, code) do
    line = recv_line(socket)
    cond do
      String.starts_with?(line, code <> "-") ->
        read_multiline(socket, code)

      String.starts_with?(line, code <> " ") ->
        :ok

      true ->
        flunk("unexpected response: #{inspect(line)}")
    end
  end

  defp recv_line(socket) do
    assert {:ok, line} = :gen_tcp.recv(socket, 0, 1000)
    line
  end

  defp connect_with_retry(host, port, attempts) do
    case :gen_tcp.connect(host, port, [:binary, active: false, packet: :line]) do
      {:ok, socket} ->
        {:ok, socket}

      {:error, _} when attempts > 1 ->
        Process.sleep(100)
        connect_with_retry(host, port, attempts - 1)

      {:error, reason} ->
        {:error, reason}
    end
  end
end
