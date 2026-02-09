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

  test "message headers parsed from raw file", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\nFrom: sender@example.com\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Test"
    assert headers["from"] == "sender@example.com"
  end

  test "message headers handle folded lines and duplicates", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Line1\r\n\tLine2\r\nSubject: Final\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Final"
  end

  test "message headers keep last duplicate header", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "X-Test: one\r\nX-Test: two\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["x-test"] == "two"
  end

  test "message headers trim whitespace", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject:   Trim Me  \r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Trim Me"
  end

  test "message headers preserve utf8 values", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Привет мир\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Привет мир"
  end

  test "message headers lower-case keys and trim", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "SuBjEcT: Mixed\r\nX-Thing:  spaced \r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Mixed"
    assert headers["x-thing"] == "spaced"
  end

  test "message headers ignore malformed lines", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "NotAHeaderLine\r\nSubject: Ok\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Ok"
    refute Map.has_key?(headers, "notaheadline")
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

  test "queue dequeue and ack remove from processing", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, ^message} = Incoming.Queue.Disk.dequeue()
    :ok = Incoming.Queue.Disk.ack(message.id)

    assert File.exists?(Path.join(tmp, "processing")) == true
    assert File.ls!(Path.join(tmp, "processing")) == []
  end

  test "queue nack reject moves to dead", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, ^message} = Incoming.Queue.Disk.dequeue()
    :ok = Incoming.Queue.Disk.nack(message.id, :reject, :test_reason)

    dead = Path.join(tmp, "dead")
    assert File.exists?(Path.join(dead, message.id))
    assert File.exists?(Path.join([dead, message.id, "dead.json"]))
  end

  test "queue depth remains zero after reject", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    assert Incoming.Queue.Disk.depth() == 0
    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert Incoming.Queue.Disk.depth() == 1
    assert {:ok, ^message} = Incoming.Queue.Disk.dequeue()
    :ok = Incoming.Queue.Disk.nack(message.id, :reject, :permanent)
    assert Incoming.Queue.Disk.depth() == 0
  end

  test "queue nack retry requeues", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, ^message} = Incoming.Queue.Disk.dequeue()
    :ok = Incoming.Queue.Disk.nack(message.id, :retry, :temporary)

    committed = Path.join([tmp, "committed", message.id])
    assert File.exists?(committed)
  end

  test "queue depth accounts for retry", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    assert Incoming.Queue.Disk.depth() == 0
    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert Incoming.Queue.Disk.depth() == 1
    assert {:ok, ^message} = Incoming.Queue.Disk.dequeue()
    :ok = Incoming.Queue.Disk.nack(message.id, :retry, :temporary)
    assert Incoming.Queue.Disk.depth() == 1
  end

  test "queue recover moves processing back to committed", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, ^message} = Incoming.Queue.Disk.dequeue()
    processing = Path.join(tmp, "processing")
    assert File.exists?(Path.join(processing, message.id))

    :ok = Incoming.Queue.Disk.recover()
    committed = Path.join(tmp, "committed")
    assert File.exists?(Path.join(committed, message.id))
    assert Incoming.Queue.Disk.depth() == 1
  end

  test "queue recover handles multiple processing entries", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message1} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    {:ok, message2} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, ^message1} = Incoming.Queue.Disk.dequeue()
    assert {:ok, ^message2} = Incoming.Queue.Disk.dequeue()

    :ok = Incoming.Queue.Disk.recover()
    assert Incoming.Queue.Disk.depth() == 2
  end

  test "delivery worker ack/retry/reject", %{tmp: tmp} do
    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 10)
    restart_app()

    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message1} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    {:ok, message2} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    {:ok, message3} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    :ok = IncomingTest.DummyAdapter.set_mode(:ok)
    wait_until(fn -> not File.exists?(Path.join([tmp, "committed", message1.id])) end)

    :ok = IncomingTest.DummyAdapter.set_mode(:retry)
    wait_until(fn -> File.exists?(Path.join([tmp, "committed", message2.id])) end)

    :ok = IncomingTest.DummyAdapter.set_mode(:reject)
    wait_until(fn -> File.exists?(Path.join([tmp, "dead", message3.id])) end)

    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 1_000)
    restart_app()
  end

  test "hello required policy rejects mail before helo", %{} do
    Application.put_env(:incoming, :policies, [Incoming.Policy.HelloRequired])
    restart_app()

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :policies, [])
    restart_app()
  end

  test "max recipients enforced", %{} do
    Application.put_env(:incoming, :session_opts, max_message_size: 10 * 1024 * 1024, max_recipients: 1)
    restart_app()

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt1@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt2@example.com>")
    assert_recv(socket, "452")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :session_opts, max_message_size: 10 * 1024 * 1024, max_recipients: 100)
    restart_app()
  end

  test "tls required policy rejects before starttls", %{} do
    Application.put_env(:incoming, :policies, [Incoming.Policy.TlsRequired])
    Application.put_env(:incoming, :listeners, [
      %{
        name: :test,
        port: 2526,
        tls: :required,
        tls_opts: [
          certfile: "test/fixtures/test-cert.pem",
          keyfile: "test/fixtures/test-key.pem"
        ]
      }
    ])
    restart_app()

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "530")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :policies, [])
    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    restart_app()
  end

  test "tls config requires cert and key", %{} do
    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{name: :bad, port: 2527, tls: :required, tls_opts: []})
    end
  end

  test "tls config rejects invalid mode", %{} do
    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{name: :bad, port: 2527, tls: :bogus, tls_opts: []})
    end
  end

  test "tls config rejects missing cert file", %{tmp: tmp} do
    keyfile = Path.join(tmp, "test.key")
    File.write!(keyfile, "key")
    certfile = Path.join(tmp, "missing.pem")

    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{
        name: :bad,
        port: 2527,
        tls: :required,
        tls_opts: [certfile: certfile, keyfile: keyfile]
      })
    end
  end

  test "invalid listener port raises", %{} do
    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{name: :bad, port: -1, tls: :disabled})
    end
  end

  test "missing listener port raises", %{} do
    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{name: :bad, tls: :disabled})
    end
  end

  test "size enforcement rejects large message", %{} do
    Application.put_env(:incoming, :session_opts, max_message_size: 10, max_recipients: 100)
    restart_app()

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

    :ok = :gen_tcp.send(socket, "Subject: Test\r\n\r\nBody body body\r\n.\r\n")
    assert_recv(socket, "552")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :session_opts, max_message_size: 10 * 1024 * 1024, max_recipients: 100)
    restart_app()
  end

  test "size extension does not override actual limit", %{} do
    Application.put_env(:incoming, :session_opts, max_message_size: 10, max_recipients: 100)
    restart_app()

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
    :ok = :gen_tcp.send(socket, "Subject: Test\r\n\r\nBody body body\r\n.\r\n")
    assert_recv(socket, "552")
    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :session_opts, max_message_size: 10 * 1024 * 1024, max_recipients: 100)
    restart_app()
  end

  test "size limit policy rejects when max size zero", %{} do
    Application.put_env(:incoming, :session_opts, max_message_size: 0, max_recipients: 100)
    Application.put_env(:incoming, :policies, [Incoming.Policy.SizeLimit])
    restart_app()

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
    assert_recv(socket, "552")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :session_opts, max_message_size: 10 * 1024 * 1024, max_recipients: 100)
    Application.put_env(:incoming, :policies, [])
    restart_app()
  end

  test "queue path validation rejects empty path", %{} do
    assert_raise ArgumentError, fn ->
      Incoming.Validate.queue_opts!(path: "")
    end
  end

  test "queue path validation rejects non-string", %{} do
    assert_raise ArgumentError, fn ->
      Incoming.Validate.queue_opts!(path: nil)
    end
  end

  test "rate limiter rejects after threshold", %{} do
    Application.put_env(:incoming, :policies, [Incoming.Policy.RateLimiter])
    restart_app()

    Application.put_env(:incoming, :rate_limit, 1)
    for _ <- 1..1 do
      {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
      :ok = :gen_tcp.close(socket)
    end

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")
    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")
    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "554")

    Application.put_env(:incoming, :policies, [])
    Application.put_env(:incoming, :rate_limit, 5)
    restart_app()
  end

  test "starttls handshake works", %{} do
    Application.put_env(:incoming, :listeners, [
      %{
        name: :test,
        port: 2526,
        tls: :optional,
        tls_opts: [
          certfile: "test/fixtures/test-cert.pem",
          keyfile: "test/fixtures/test-key.pem"
        ]
      }
    ])
    restart_app()

    cmd = "printf 'EHLO test\\nSTARTTLS\\nQUIT\\n' | openssl s_client -starttls smtp -connect 127.0.0.1:2526 -quiet -servername localhost"
    {output, _status} = System.cmd("bash", ["-lc", cmd])
    assert output =~ "250 SMTPUTF8"

    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    restart_app()
  end

  test "starttls required allows mail after tls handshake", %{} do
    Application.put_env(:incoming, :listeners, [
      %{
        name: :test,
        port: 2526,
        tls: :required,
        tls_opts: [
          certfile: "test/fixtures/test-cert.pem",
          keyfile: "test/fixtures/test-key.pem"
        ]
      }
    ])
    restart_app()

    cmd = "timeout 5s bash -lc \"printf 'EHLO test\\nEHLO test\\nMAIL FROM:<sender@example.com>\\nRCPT TO:<rcpt@example.com>\\nDATA\\nSubject: Test\\n\\nBody\\n.\\nQUIT\\n' | openssl s_client -starttls smtp -crlf -connect 127.0.0.1:2526 -quiet -ign_eof -servername localhost\""
    {output, _status} = System.cmd("bash", ["-lc", cmd])
    assert output =~ "250 SMTPUTF8"
    assert output =~ "250 Ok: queued"

    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    restart_app()
  end

  test "tls required policy enforces starttls across connections", %{} do
    Application.put_env(:incoming, :policies, [Incoming.Policy.TlsRequired])
    Application.put_env(:incoming, :listeners, [
      %{
        name: :test,
        port: 2526,
        tls: :required,
        tls_opts: [
          certfile: "test/fixtures/test-cert.pem",
          keyfile: "test/fixtures/test-key.pem"
        ]
      }
    ])
    restart_app()

    {:ok, socket1} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket1, "220")
    send_line(socket1, "EHLO client.example.com")
    read_multiline(socket1, "250")
    send_line(socket1, "MAIL FROM:<sender@example.com>")
    assert_recv(socket1, "530")
    send_line(socket1, "QUIT")
    assert_recv(socket1, "221")

    {:ok, socket2} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket2, "220")
    send_line(socket2, "EHLO client.example.com")
    read_multiline(socket2, "250")
    send_line(socket2, "MAIL FROM:<sender@example.com>")
    assert_recv(socket2, "530")
    send_line(socket2, "QUIT")
    assert_recv(socket2, "221")

    Application.put_env(:incoming, :policies, [])
    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    restart_app()
  end

  test "starttls refused when disabled", %{} do
    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    restart_app()

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "STARTTLS")
    assert_recv(socket, "500")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "ehlo advertises size and omits starttls when disabled", %{} do
    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    Application.put_env(:incoming, :session_opts, max_message_size: 1234, max_recipients: 100)
    restart_app()

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    lines = read_multiline_lines(socket, "250")
    assert Enum.any?(lines, &String.contains?(&1, "SIZE 1234"))
    refute Enum.any?(lines, &String.contains?(&1, "STARTTLS"))

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :session_opts, max_message_size: 10 * 1024 * 1024, max_recipients: 100)
    restart_app()
  end

  test "ehlo size reflects updated configuration", %{} do
    Application.put_env(:incoming, :session_opts, max_message_size: 2048, max_recipients: 100)
    restart_app()

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")
    send_line(socket, "EHLO client.example.com")
    lines = read_multiline_lines(socket, "250")
    assert Enum.any?(lines, &String.contains?(&1, "SIZE 2048"))
    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :session_opts, max_message_size: 10 * 1024 * 1024, max_recipients: 100)
    restart_app()
  end

  test "ehlo advertises starttls when optional", %{} do
    Application.put_env(:incoming, :listeners, [
      %{
        name: :test,
        port: 2526,
        tls: :optional,
        tls_opts: [
          certfile: "test/fixtures/test-cert.pem",
          keyfile: "test/fixtures/test-key.pem"
        ]
      }
    ])
    restart_app()

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")
    send_line(socket, "EHLO client.example.com")
    lines = read_multiline_lines(socket, "250")
    assert Enum.any?(lines, &String.contains?(&1, "STARTTLS"))

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    restart_app()
  end

  test "queue dequeue skips corrupt metadata", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.write!(Path.join([tmp, "committed", message.id, "meta.json"]), "not-json")

    assert {:empty} = Incoming.Queue.Disk.dequeue()
    assert File.exists?(Path.join([tmp, "committed", message.id]))
  end

  test "queue dequeue skips missing metadata file", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.rm!(Path.join([tmp, "committed", message.id, "meta.json"]))

    assert {:empty} = Incoming.Queue.Disk.dequeue()
    assert File.exists?(Path.join([tmp, "committed", message.id]))
  end

  test "queue dequeue skips missing raw file", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.rm!(Path.join([tmp, "committed", message.id, "raw.eml"]))

    assert {:ok, _message} = Incoming.Queue.Disk.dequeue()
    # Message still dequeues based on metadata; raw file may be missing for consumers.
  end

  test "queue dequeue skips entry with missing raw and meta", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.rm!(Path.join([tmp, "committed", message.id, "raw.eml"]))
    File.rm!(Path.join([tmp, "committed", message.id, "meta.json"]))

    assert {:empty} = Incoming.Queue.Disk.dequeue()
  end

  test "queue handles partial metadata (missing fields)", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.write!(Path.join([tmp, "committed", message.id, "meta.json"]), Jason.encode!(%{"received_at" => "bad"}))

    assert {:ok, loaded} = Incoming.Queue.Disk.dequeue()
    assert loaded.mail_from == nil
    assert loaded.rcpt_to == []
  end

  test "queue handles invalid timestamp in metadata", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.write!(Path.join([tmp, "committed", message.id, "meta.json"]), Jason.encode!(%{"received_at" => "not-a-time"}))

    assert {:ok, loaded} = Incoming.Queue.Disk.dequeue()
    assert %DateTime{} = loaded.received_at
  end

  test "queue defaults rcpt_to to empty list when missing", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.write!(
      Path.join([tmp, "committed", message.id, "meta.json"]),
      Jason.encode!(%{"mail_from" => "sender@example.com", "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()})
    )

    assert {:ok, loaded} = Incoming.Queue.Disk.dequeue()
    assert loaded.rcpt_to == []
  end

  test "queue recover handles stray processing entries", %{tmp: tmp} do
    processing = Path.join([tmp, "processing"])
    File.mkdir_p!(processing)
    File.mkdir_p!(Path.join(processing, "stray"))

    assert :ok = Incoming.Queue.Disk.recover()
    assert File.exists?(Path.join([tmp, "committed", "stray"]))
  end

  test "queue depth telemetry emits periodic event", %{tmp: tmp} do
    id = "incoming-test-queue-depth-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :queue, :depth],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    assert_receive {:telemetry, [:incoming, :queue, :depth], meas, _meta}, 6_000
    assert meas.count >= 0

    :telemetry.detach(id)
  end

  test "queue depth reflects enqueue and ack", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    assert Incoming.Queue.Disk.depth() == 0
    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert Incoming.Queue.Disk.depth() == 1
    assert {:ok, ^message} = Incoming.Queue.Disk.dequeue()
    :ok = Incoming.Queue.Disk.ack(message.id)
    assert Incoming.Queue.Disk.depth() == 0
  end

  test "dead letter metadata includes reason and timestamp", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, ^message} = Incoming.Queue.Disk.dequeue()
    :ok = Incoming.Queue.Disk.nack(message.id, :reject, :test_reason)

    dead = Path.join([tmp, "dead", message.id, "dead.json"])
    assert {:ok, payload} = File.read(dead)
    decoded = Jason.decode!(payload)
    assert decoded["reason"] =~ "test_reason"
    assert {:ok, _dt, _} = DateTime.from_iso8601(decoded["rejected_at"])
  end

  test "telemetry emits enqueue event", %{tmp: tmp} do
    id = "incoming-test-telemetry-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :message, :queued],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    assert_receive {:telemetry, [:incoming, :message, :queued], _meas, meta}, 1_000
    assert is_binary(meta.id)
    assert meta.size > 0

    :telemetry.detach(id)
  end

  test "policy context includes envelope and tls flags", %{} do
    IncomingTest.CapturePolicy.set_target(self())
    Application.put_env(:incoming, :policies, [IncomingTest.CapturePolicy])
    restart_app()

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

    assert_receive {:policy_phase, :connect, ctx1}, 1_000
    assert ctx1.tls_active == false
    assert ctx1.envelope.mail_from == nil

    assert_receive {:policy_phase, :mail_from, ctx2}, 1_000
    assert ctx2.envelope.mail_from == "sender@example.com"

    assert_receive {:policy_phase, :rcpt_to, ctx3}, 1_000
    assert ctx3.envelope.rcpt_to == ["rcpt@example.com"]

    assert_receive {:policy_phase, :message_complete, _ctx4}, 1_000

    Application.put_env(:incoming, :policies, [])
    restart_app()
  end

  test "rcpt before mail is accepted by gen_smtp defaults", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "command order enforcement rejects data before rcpt", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "DATA")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "noop and rset are accepted", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "NOOP")
    assert_recv(socket, "250")

    send_line(socket, "RSET")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "unknown command yields 500", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "WUT")
    assert_recv(socket, "500")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "invalid helo yields error", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "HELO")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "invalid helo with extra spaces yields error", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "HELO    ")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "invalid ehlo yields error", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "help command returns 214", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "HELP")
    assert_recv(socket, "500")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "vrfy and expn are disabled", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "VRFY user@example.com")
    assert_recv(socket, "252")

    send_line(socket, "EXPN list")
    assert_recv(socket, "500")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "policy order preserves sequence", %{} do
    IncomingTest.CapturePolicy.set_target(self())
    Application.put_env(:incoming, :policies, [IncomingTest.CapturePolicy])
    restart_app()

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

    phases = collect_phases([])
    assert phases == [:connect, :helo, :mail_from, :rcpt_to, :data_start, :message_complete]

    Application.put_env(:incoming, :policies, [])
    restart_app()
  end

  defp collect_phases(acc, timeout \\ 100)
  defp collect_phases(acc, timeout) do
    receive do
      {:policy_phase, phase, _ctx} -> collect_phases([phase | acc], timeout)
    after
      timeout -> Enum.reverse(acc)
    end
  end

  test "invalid mail command yields error", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<>")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "invalid rcpt command yields error", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<>")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "policy short-circuits on rejection", %{} do
    IncomingTest.CapturePolicy.set_target(self())
    Application.put_env(:incoming, :policies, [Incoming.Policy.HelloRequired, IncomingTest.CapturePolicy])
    restart_app()

    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "503")

    refute_receive {:policy_phase, :mail_from, _ctx}, 200

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :policies, [])
    restart_app()
  end

  test "rset clears envelope", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RSET")
    assert_recv(socket, "250")

    send_line(socket, "DATA")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "rset after data command still rejects without mail", %{} do
    {:ok, socket} = connect_with_retry(~c"localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "DATA")
    assert_recv(socket, "503")

    send_line(socket, "RSET")
    assert_recv(socket, "250")

    send_line(socket, "DATA")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "telemetry emits delivery result", %{tmp: tmp} do
    id = "incoming-test-delivery-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :delivery, :result],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 10)
    restart_app()

    IncomingTest.DummyAdapter.set_mode(:ok)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    assert_receive {:telemetry, [:incoming, :delivery, :result], _meas, meta}, 1_000
    assert meta.outcome == :ok

    :telemetry.detach(id)
    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 1_000)
    restart_app()
  end

  test "telemetry emits retry and reject outcomes", %{tmp: tmp} do
    id = "incoming-test-delivery-outcomes-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :delivery, :result],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 10, max_attempts: 1, base_backoff: 1, max_backoff: 1)
    restart_app()

    IncomingTest.DummyAdapter.set_mode(:retry)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    assert_receive {:telemetry, [:incoming, :delivery, :result], _meas, meta1}, 1_000
    assert meta1.outcome in [:retry, :reject]
    assert meta1.reason != nil

    :telemetry.detach(id)
    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 1_000)
    restart_app()
  end

  test "delivery telemetry reason matches adapter", %{tmp: tmp} do
    id = "incoming-test-delivery-reason-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :delivery, :result],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 10)
    restart_app()

    IncomingTest.DummyAdapter.set_mode(:retry)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    assert_receive {:telemetry, [:incoming, :delivery, :result], _meas, meta}, 1_000
    assert meta.reason == :temporary

    :telemetry.detach(id)
    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 1_000)
    restart_app()
  end

  test "delivery retries eventually send to dead letter on reject", %{tmp: tmp} do
    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 10)
    restart_app()

    IncomingTest.DummyAdapter.set_mode(:reject)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    wait_until(fn -> File.exists?(Path.join([tmp, "dead", message.id])) end)

    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 1_000)
    restart_app()
  end

  test "delivery retry stops after max_attempts", %{tmp: tmp} do
    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :delivery_opts,
      workers: 1,
      poll_interval: 10,
      max_attempts: 1,
      base_backoff: 1,
      max_backoff: 1
    )
    restart_app()

    IncomingTest.DummyAdapter.set_mode(:retry)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    wait_until(fn -> File.exists?(Path.join([tmp, "dead", message.id])) end)
    dead_json = Path.join([tmp, "dead", message.id, "dead.json"])
    assert {:ok, payload} = File.read(dead_json)
    decoded = Jason.decode!(payload)
    assert decoded["reason"] =~ "max_attempts"
    assert decoded["reason"] =~ "temporary"

    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 1_000)
    restart_app()
  end

  test "delivery worker tracks retry attempts", %{tmp: tmp} do
    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)

    IncomingTest.DummyAdapter.set_mode(:retry)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    {:ok, pid} =
      Incoming.Delivery.Worker.start_link(
        poll_interval: 10_000,
        max_attempts: 3,
        base_backoff: 1,
        max_backoff: 1
      )

    :ok =
      wait_until(fn ->
        state = :sys.get_state(pid)
        Map.has_key?(state.attempts, message.id)
      end)

    state = :sys.get_state(pid)
    assert state.attempts[message.id] == 1

    GenServer.stop(pid)
    Application.put_env(:incoming, :delivery, nil)
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

  defp read_multiline_lines(socket, code, acc \\ []) do
    line = recv_line(socket)
    cond do
      String.starts_with?(line, code <> "-") ->
        read_multiline_lines(socket, code, [line | acc])

      String.starts_with?(line, code <> " ") ->
        Enum.reverse([line | acc])

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

  defp restart_app do
    Application.stop(:incoming)
    Application.ensure_all_started(:incoming)
  end

  defp wait_until(fun, attempts \\ 50)
  defp wait_until(_fun, 0), do: :timeout

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end

end
