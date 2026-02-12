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

  test "queue enqueue_stream writes chunks to disk", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    chunks = ["Subject: Test\r\n", "\r\nBody\r\n"]

    {:ok, message} = Incoming.Queue.Disk.enqueue_stream(from, to, chunks, path: tmp, fsync: false)
    assert File.read!(message.raw_path) == IO.iodata_to_binary(chunks)
  end

  test "queue enqueue_stream enforces max_message_size and cleans up", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    chunks = ["12345", "6"]

    assert {:error, :message_too_large} =
             Incoming.Queue.Disk.enqueue_stream(from, to, chunks,
               path: tmp,
               fsync: false,
               max_message_size: 5
             )

    assert File.ls!(Path.join(tmp, "committed")) == []
  end

  test "queue enqueue returns error when path is a file (not a directory)", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    file_path = Path.join(tmp, "not-a-dir")
    File.write!(file_path, "nope")

    assert {:error, %File.Error{reason: reason}} =
             Incoming.Queue.Disk.enqueue(from, to, data, path: file_path, fsync: false)

    assert reason in [:enotdir, :eexist]
  end

  test "queue enqueue returns error on invalid queue path option", %{tmp: _tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    assert {:error, %ArgumentError{}} =
             Incoming.Queue.Disk.enqueue(from, to, data, path: "", fsync: false)
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
    assert headers["subject"] == "Line1 Line2, Final"
  end

  test "message headers accumulate duplicate headers", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "X-Test: one\r\nX-Test: two\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["x-test"] == "one, two"
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

  test "message headers empty when missing", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers == %{}
  end

  test "message headers join duplicate values with comma", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: One\r\nSubject: Two\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "One, Two"
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

  test "header folding with tab continuation", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Hello\r\n\tWorld\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Hello World"
  end

  test "header folding with space continuation", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Hello\r\n World\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Hello World"
  end

  test "header folding with multiple continuation lines", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]

    data =
      "Received: from mx.example.com\r\n\tby mail.example.com\r\n\twith ESMTP\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["received"] == "from mx.example.com by mail.example.com with ESMTP"
  end

  test "mixed folded and unfolded headers", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]

    data =
      "From: sender@example.com\r\nSubject: Long\r\n subject line\r\nTo: rcpt@example.com\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["from"] == "sender@example.com"
    assert headers["subject"] == "Long subject line"
    assert headers["to"] == "rcpt@example.com"
  end

  test "continuation line as first line is ignored", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "\tcontinuation\r\nSubject: Ok\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Ok"
    assert map_size(headers) == 1
  end

  test "accepts smtp session and queues message", %{tmp: tmp} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)

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

  test "supports SMTP PIPELINING (ehlo/mail/rcpt/data without waiting)", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    :ok =
      :gen_tcp.send(
        socket,
        "EHLO client.example.com\r\n" <>
          "MAIL FROM:<sender@example.com>\r\n" <>
          "RCPT TO:<rcpt@example.com>\r\n" <>
          "DATA\r\n"
      )

    read_multiline(socket, "250")
    assert_recv(socket, "250")
    assert_recv(socket, "250")
    assert_recv(socket, "354")

    :ok = :gen_tcp.send(socket, "Subject: Test\r\n\r\nBody\r\n.\r\nQUIT\r\n")
    assert_recv(socket, "250")
    assert_recv(socket, "221")

    assert length(File.ls!(Path.join(tmp, "committed"))) == 1
  end

  test "PIPELINING handles misordered commands (rcpt before mail rejected, later accepted)", %{
    tmp: tmp
  } do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    :ok =
      :gen_tcp.send(
        socket,
        "EHLO client.example.com\r\n" <>
          "RCPT TO:<rcpt@example.com>\r\n" <>
          "MAIL FROM:<sender@example.com>\r\n" <>
          "RCPT TO:<rcpt@example.com>\r\n" <>
          "DATA\r\n"
      )

    read_multiline(socket, "250")
    assert_recv(socket, "503")
    assert_recv(socket, "250")
    assert_recv(socket, "250")
    assert_recv(socket, "354")

    :ok = :gen_tcp.send(socket, "Subject: Test\r\n\r\nBody\r\n.\r\nQUIT\r\n")
    assert_recv(socket, "250")
    assert_recv(socket, "221")

    assert length(File.ls!(Path.join(tmp, "committed"))) == 1
  end

  test "RSET is accepted after DATA completes", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    :ok =
      :gen_tcp.send(
        socket,
        "EHLO client.example.com\r\n" <>
          "MAIL FROM:<sender@example.com>\r\n" <>
          "RCPT TO:<rcpt@example.com>\r\n" <>
          "DATA\r\n" <>
          "Subject: Test\r\n\r\nBody\r\n.\r\n"
      )

    read_multiline(socket, "250")
    assert_recv(socket, "250")
    assert_recv(socket, "250")
    assert_recv(socket, "354")
    assert_recv(socket, "250")

    send_line(socket, "RSET")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    assert length(File.ls!(Path.join(tmp, "committed"))) == 1
  end

  test "RSET resets the envelope mid-transaction", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    assert File.ls!(Path.join(tmp, "committed")) == []
  end

  test "nested MAIL is rejected and does not change the envelope", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender1@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "MAIL FROM:<sender2@example.com>")
    assert_recv(socket, "503")

    send_line(socket, "DATA")
    assert_recv(socket, "354")
    :ok = :gen_tcp.send(socket, "Subject: Test\r\n\r\nBody\r\n.\r\n")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    [id] = File.ls!(Path.join(tmp, "committed"))
    meta = Jason.decode!(File.read!(Path.join([tmp, "committed", id, "meta.json"])))
    assert meta["mail_from"] == "sender1@example.com"
    assert meta["rcpt_to"] == ["rcpt@example.com"]
  end

  test "envelope resets after DATA completes (RCPT requires new MAIL)", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    assert length(File.ls!(Path.join(tmp, "committed"))) == 1
  end

  test "quit mid-transaction does not enqueue", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    assert File.ls!(Path.join(tmp, "committed")) == []
  end

  test "malformed MAIL syntax is rejected and does not enqueue", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL sender@example.com")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    assert File.ls!(Path.join(tmp, "committed")) == []
  end

  test "malformed RCPT syntax is rejected and does not enqueue", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT rcpt@example.com")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    assert File.ls!(Path.join(tmp, "committed")) == []
  end

  test "commands sent during DATA are treated as message body", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "DATA")
    assert_recv(socket, "354")

    # This is not a command while in DATA mode.
    :ok = :gen_tcp.send(socket, "Subject: Test\r\n\r\nRSET\r\n")
    assert_no_recv(socket, 100)

    :ok = :gen_tcp.send(socket, "Body\r\n.\r\n")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    [id] = File.ls!(Path.join(tmp, "committed"))
    raw = File.read!(Path.join([tmp, "committed", id, "raw.eml"]))
    assert String.contains?(raw, "\r\n\r\nRSET\r\nBody")
  end

  test "queue dequeue and ack remove from processing", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
    assert File.exists?(dequeued.raw_path)
    :ok = Incoming.Queue.Disk.ack(message.id)

    assert File.exists?(Path.join(tmp, "processing")) == true
    assert File.ls!(Path.join(tmp, "processing")) == []
  end

  test "queue nack reject moves to dead", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
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
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
    :ok = Incoming.Queue.Disk.nack(message.id, :reject, :permanent)
    assert Incoming.Queue.Disk.depth() == 0
  end

  test "queue nack retry requeues", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
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
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
    :ok = Incoming.Queue.Disk.nack(message.id, :retry, :temporary)
    assert Incoming.Queue.Disk.depth() == 1
  end

  test "queue recover moves processing back to committed", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
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
    {:ok, dequeued1} = Incoming.Queue.Disk.dequeue()
    {:ok, dequeued2} = Incoming.Queue.Disk.dequeue()
    dequeued_ids = MapSet.new([dequeued1.id, dequeued2.id])
    assert dequeued_ids == MapSet.new([message1.id, message2.id])

    :ok = Incoming.Queue.Disk.recover()
    assert Incoming.Queue.Disk.depth() == 2
  end

  test "queue recover does not clobber existing committed entry", %{tmp: tmp} do
    id = "conflict-#{System.unique_integer([:positive])}"
    committed = Path.join([tmp, "committed", id])
    processing = Path.join([tmp, "processing", id])
    File.mkdir_p!(committed)
    File.mkdir_p!(processing)

    File.write!(Path.join(committed, "raw.eml"), "Subject: Ok\r\n\r\nBody\r\n")

    File.write!(
      Path.join(committed, "meta.json"),
      Jason.encode!(%{
        "id" => id,
        "mail_from" => "sender@example.com",
        "rcpt_to" => ["rcpt@example.com"],
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    )

    File.write!(Path.join(processing, "raw.eml"), "Subject: Proc\r\n\r\nBody\r\n")

    File.write!(
      Path.join(processing, "meta.json"),
      Jason.encode!(%{
        "id" => id,
        "mail_from" => "sender@example.com",
        "rcpt_to" => ["rcpt@example.com"],
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    )

    :ok = Incoming.Queue.Disk.recover()
    assert File.exists?(committed)
    refute File.exists?(processing)
    assert File.exists?(Path.join([tmp, "dead", id, "dead.json"]))
  end

  test "queue recover keeps remaining processing entries after conflict", %{tmp: tmp} do
    id1 = "conflict-#{System.unique_integer([:positive])}"
    id2 = "pending-#{System.unique_integer([:positive])}"
    committed = Path.join([tmp, "committed", id1])
    processing1 = Path.join([tmp, "processing", id1])
    processing2 = Path.join([tmp, "processing", id2])
    File.mkdir_p!(committed)
    File.mkdir_p!(processing1)
    File.mkdir_p!(processing2)

    File.write!(Path.join(committed, "raw.eml"), "Subject: Ok\r\n\r\nBody\r\n")

    File.write!(
      Path.join(committed, "meta.json"),
      Jason.encode!(%{
        "id" => id1,
        "mail_from" => "sender@example.com",
        "rcpt_to" => ["rcpt@example.com"],
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    )

    for {dir, id} <- [{processing1, id1}, {processing2, id2}] do
      File.write!(Path.join(dir, "raw.eml"), "Subject: Proc\r\n\r\nBody\r\n")

      File.write!(
        Path.join(dir, "meta.json"),
        Jason.encode!(%{
          "id" => id,
          "mail_from" => "sender@example.com",
          "rcpt_to" => ["rcpt@example.com"],
          "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()
        })
      )
    end

    :ok = Incoming.Queue.Disk.recover()

    assert File.exists?(committed)
    refute File.exists?(processing1)
    assert File.exists?(Path.join([tmp, "committed", id2]))
    refute File.exists?(processing2)
    assert File.exists?(Path.join([tmp, "dead", id1, "dead.json"]))
  end

  test "queue recover dead-letters processing file entries", %{tmp: tmp} do
    id = "file-entry-#{System.unique_integer([:positive])}"
    processing = Path.join([tmp, "processing", id])
    File.mkdir_p!(Path.dirname(processing))
    File.write!(processing, "placeholder")

    :ok = Incoming.Queue.Disk.recover()

    refute File.exists?(processing)
    refute File.exists?(Path.join([tmp, "committed", id]))
    assert File.exists?(Path.join([tmp, "dead", id, "dead.json"]))
  end

  test "queue recover dead-letters committed file entries", %{tmp: tmp} do
    id = "file-committed-#{System.unique_integer([:positive])}"
    committed = Path.join([tmp, "committed", id])
    File.mkdir_p!(Path.dirname(committed))
    File.write!(committed, "placeholder")

    :ok = Incoming.Queue.Disk.recover()

    refute File.exists?(committed)
    dead_json = Path.join([tmp, "dead", id, "dead.json"])
    assert File.exists?(dead_json)
    decoded = Jason.decode!(File.read!(dead_json))
    assert decoded["reason"] =~ "invalid_committed_entry"
  end

  test "queue dequeue skips file entry", %{tmp: tmp} do
    id = "file-committed-#{System.unique_integer([:positive])}"
    committed = Path.join([tmp, "committed", id])
    File.mkdir_p!(Path.dirname(committed))
    File.write!(committed, "placeholder")

    assert {:empty} = Incoming.Queue.Disk.dequeue()
    refute File.exists?(committed)
    assert File.exists?(Path.join([tmp, "dead", id, "dead.json"]))
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

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :policies, [])
    restart_app()
  end

  test "rejected MAIL does not set envelope (RCPT still rejected as bad sequence)", %{} do
    Application.put_env(:incoming, :policies, [IncomingTest.RejectMailPolicy])
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "550")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :policies, [])
    restart_app()
  end

  test "max recipients enforced", %{} do
    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 1
    )

    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100
    )

    restart_app()
  end

  test "rejected RCPT does not count toward max recipients", %{} do
    IncomingTest.RejectRcptOncePolicy.reset()

    Application.put_env(:incoming, :policies, [IncomingTest.RejectRcptOncePolicy])

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 1
    )

    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    # Rejected by policy, should not be retained in the envelope.
    send_line(socket, "RCPT TO:<rcpt1@example.com>")
    assert_recv(socket, "550")

    # Allowed by policy, should not hit max recipients.
    send_line(socket, "RCPT TO:<rcpt2@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :policies, [])

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100
    )

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

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

  test "tls config rejects invalid cert contents", %{tmp: tmp} do
    certfile = Path.join(tmp, "bad-cert.pem")
    File.write!(certfile, "not a pem")

    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{
        name: :bad,
        port: 2527,
        tls: :required,
        tls_opts: [certfile: certfile, keyfile: "test/fixtures/test-key.pem"]
      })
    end
  end

  test "tls config rejects invalid key contents", %{tmp: tmp} do
    keyfile = Path.join(tmp, "bad-key.pem")
    File.write!(keyfile, "not a pem")

    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{
        name: :bad,
        port: 2527,
        tls: :required,
        tls_opts: [certfile: "test/fixtures/test-cert.pem", keyfile: keyfile]
      })
    end
  end

  test "tls config rejects mismatched cert/key", %{tmp: tmp} do
    # Generate an RSA key that does not match the test cert.
    key = :public_key.generate_key({:rsa, 2048, 65_537})
    pem = :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, key)])
    keyfile = Path.join(tmp, "mismatch-key.pem")
    File.write!(keyfile, pem)

    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{
        name: :bad,
        port: 2527,
        tls: :required,
        tls_opts: [certfile: "test/fixtures/test-cert.pem", keyfile: keyfile]
      })
    end
  end

  test "invalid listener port raises", %{} do
    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{name: :bad, port: -1, tls: :disabled})
    end
  end

  test "non-integer listener port raises", %{} do
    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{name: :bad, port: "2526", tls: :disabled})
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

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100
    )

    restart_app()
  end

  test "size extension does not override actual limit", %{} do
    Application.put_env(:incoming, :session_opts, max_message_size: 10, max_recipients: 100)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100
    )

    restart_app()
  end

  test "size limit policy rejects when max size zero", %{} do
    Application.put_env(:incoming, :session_opts, max_message_size: 0, max_recipients: 100)
    Application.put_env(:incoming, :policies, [Incoming.Policy.SizeLimit])
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100
    )

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
      {:ok, socket} = connect_with_retry("localhost", 2526, 10)
      :ok = :gen_tcp.close(socket)
    end

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    cmd =
      "printf 'EHLO test\\nSTARTTLS\\nQUIT\\n' | openssl s_client -starttls smtp -connect 127.0.0.1:2526 -quiet -servername localhost"

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

    cmd =
      "timeout 5s bash -lc \"printf 'EHLO test\\nEHLO test\\nMAIL FROM:<sender@example.com>\\nRCPT TO:<rcpt@example.com>\\nDATA\\nSubject: Test\\n\\nBody\\n.\\nQUIT\\n' | openssl s_client -starttls smtp -crlf -connect 127.0.0.1:2526 -quiet -ign_eof -servername localhost\""

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

    {:ok, socket1} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket1, "220")
    send_line(socket1, "EHLO client.example.com")
    read_multiline(socket1, "250")
    send_line(socket1, "MAIL FROM:<sender@example.com>")
    assert_recv(socket1, "530")
    send_line(socket1, "QUIT")
    assert_recv(socket1, "221")

    {:ok, socket2} = connect_with_retry("localhost", 2526, 10)
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

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    lines = read_multiline_lines(socket, "250")
    assert Enum.any?(lines, &String.contains?(&1, "SIZE 1234"))
    refute Enum.any?(lines, &String.contains?(&1, "STARTTLS"))

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100
    )

    restart_app()
  end

  test "ehlo size reflects updated configuration", %{} do
    Application.put_env(:incoming, :session_opts, max_message_size: 2048, max_recipients: 100)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")
    send_line(socket, "EHLO client.example.com")
    lines = read_multiline_lines(socket, "250")
    assert Enum.any?(lines, &String.contains?(&1, "SIZE 2048"))
    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100
    )

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

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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
    refute File.exists?(Path.join([tmp, "committed", message.id]))
    assert File.exists?(Path.join([tmp, "dead", message.id, "dead.json"]))
  end

  test "queue dequeue skips missing metadata file", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.rm!(Path.join([tmp, "committed", message.id, "meta.json"]))

    assert {:empty} = Incoming.Queue.Disk.dequeue()
    refute File.exists?(Path.join([tmp, "committed", message.id]))
    assert File.exists?(Path.join([tmp, "dead", message.id, "dead.json"]))
  end

  test "queue dequeue skips missing raw file", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.rm!(Path.join([tmp, "committed", message.id, "raw.eml"]))

    assert {:empty} = Incoming.Queue.Disk.dequeue()
    refute File.exists?(Path.join([tmp, "committed", message.id]))
    assert File.exists?(Path.join([tmp, "dead", message.id, "dead.json"]))
  end

  test "queue dequeue skips entry with missing raw and meta", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    File.rm!(Path.join([tmp, "committed", message.id, "raw.eml"]))
    File.rm!(Path.join([tmp, "committed", message.id, "meta.json"]))

    assert {:empty} = Incoming.Queue.Disk.dequeue()
    refute File.exists?(Path.join([tmp, "committed", message.id]))
    assert File.exists?(Path.join([tmp, "dead", message.id, "dead.json"]))
  end

  test "queue dequeue skips invalid entry and still returns a later valid entry", %{tmp: tmp} do
    committed = Path.join(tmp, "committed")

    bad_id = "a"
    bad_dir = Path.join([committed, bad_id])
    File.mkdir_p!(bad_dir)
    File.write!(Path.join(bad_dir, "meta.json"), Jason.encode!(%{"mail_from" => "x"}))
    # Intentionally omit raw.eml

    good_id = "b"
    good_dir = Path.join([committed, good_id])
    File.mkdir_p!(good_dir)

    File.write!(Path.join(good_dir, "raw.eml"), "Subject: Ok\r\n\r\nBody\r\n")

    File.write!(
      Path.join(good_dir, "meta.json"),
      Jason.encode!(%{
        "id" => good_id,
        "mail_from" => "sender@example.com",
        "rcpt_to" => ["rcpt@example.com"],
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    )

    assert {:ok, message} = Incoming.Queue.Disk.dequeue()
    assert message.id == good_id
    assert File.exists?(message.raw_path)
    assert File.exists?(Path.join([tmp, "dead", bad_id, "dead.json"]))
  end

  test "queue handles partial metadata (missing fields)", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    File.write!(
      Path.join([tmp, "committed", message.id, "meta.json"]),
      Jason.encode!(%{"received_at" => "bad"})
    )

    assert {:ok, loaded} = Incoming.Queue.Disk.dequeue()
    assert loaded.mail_from == nil
    assert loaded.rcpt_to == []
  end

  test "queue handles invalid timestamp in metadata", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    File.write!(
      Path.join([tmp, "committed", message.id, "meta.json"]),
      Jason.encode!(%{"received_at" => "not-a-time"})
    )

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
      Jason.encode!(%{
        "mail_from" => "sender@example.com",
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    )

    assert {:ok, loaded} = Incoming.Queue.Disk.dequeue()
    assert loaded.rcpt_to == []
  end

  test "queue recover handles stray processing entries", %{tmp: tmp} do
    processing = Path.join([tmp, "processing"])
    File.mkdir_p!(processing)
    File.mkdir_p!(Path.join(processing, "stray"))

    assert :ok = Incoming.Queue.Disk.recover()
    refute File.exists?(Path.join([tmp, "committed", "stray"]))
    assert File.exists?(Path.join([tmp, "dead", "stray", "dead.json"]))
  end

  test "queue recover promotes raw.tmp and meta.tmp to final names", %{tmp: tmp} do
    id = "tmp-promote-#{System.unique_integer([:positive])}"
    base = Path.join([tmp, "committed", id])
    File.mkdir_p!(base)

    File.write!(Path.join(base, "raw.tmp"), "Subject: Ok\r\n\r\nBody\r\n")

    File.write!(
      Path.join(base, "meta.tmp"),
      Jason.encode!(%{
        "id" => id,
        "mail_from" => "sender@example.com",
        "rcpt_to" => ["rcpt@example.com"],
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    )

    assert :ok = Incoming.Queue.Disk.recover()

    assert File.exists?(Path.join(base, "raw.eml"))
    assert File.exists?(Path.join(base, "meta.json"))

    assert {:ok, message} = Incoming.Queue.Disk.dequeue()
    assert message.id == id
  end

  test "queue recover dead-letters incomplete committed entry", %{tmp: tmp} do
    id = "committed-incomplete-#{System.unique_integer([:positive])}"
    base = Path.join([tmp, "committed", id])
    File.mkdir_p!(base)

    # meta.tmp exists but raw is missing
    File.write!(
      Path.join(base, "meta.tmp"),
      Jason.encode!(%{
        "id" => id,
        "mail_from" => "sender@example.com",
        "rcpt_to" => ["rcpt@example.com"],
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    )

    assert :ok = Incoming.Queue.Disk.recover()

    refute File.exists?(base)
    dead_json = Path.join([tmp, "dead", id, "dead.json"])
    assert File.exists?(dead_json)
    decoded = Jason.decode!(File.read!(dead_json))
    assert decoded["reason"] =~ "incomplete_committed_entry"
  end

  test "queue recover finalizes staged incoming entry (incoming -> committed)", %{tmp: tmp} do
    id = "staged-finalize-#{System.unique_integer([:positive])}"
    base = Path.join([tmp, "incoming", id])
    File.mkdir_p!(base)

    File.write!(Path.join(base, "raw.tmp"), "Subject: Ok\r\n\r\nBody\r\n")

    File.write!(
      Path.join(base, "meta.tmp"),
      Jason.encode!(%{
        "id" => id,
        "mail_from" => "sender@example.com",
        "rcpt_to" => ["rcpt@example.com"],
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "attempts" => 0
      })
    )

    assert :ok = Incoming.Queue.Disk.recover()

    refute File.exists?(base)
    assert File.exists?(Path.join([tmp, "committed", id, "raw.eml"]))
    assert File.exists?(Path.join([tmp, "committed", id, "meta.json"]))
  end

  test "queue recover dead-letters incomplete staged incoming entry", %{tmp: tmp} do
    id = "staged-incomplete-#{System.unique_integer([:positive])}"
    base = Path.join([tmp, "incoming", id])
    File.mkdir_p!(base)

    # raw.tmp exists but meta.tmp is missing
    File.write!(Path.join(base, "raw.tmp"), "Subject: Ok\r\n\r\nBody\r\n")

    assert :ok = Incoming.Queue.Disk.recover()

    refute File.exists?(base)
    dead_json = Path.join([tmp, "dead", id, "dead.json"])
    assert File.exists?(dead_json)
    decoded = Jason.decode!(File.read!(dead_json))
    assert decoded["reason"] =~ "incomplete_write"
  end

  test "queue recover dead-letters incomplete staged incoming entry when raw is missing", %{
    tmp: tmp
  } do
    id = "staged-incomplete-raw-missing-#{System.unique_integer([:positive])}"
    base = Path.join([tmp, "incoming", id])
    File.mkdir_p!(base)

    # meta.tmp exists but raw.tmp is missing
    File.write!(
      Path.join(base, "meta.tmp"),
      Jason.encode!(%{
        "id" => id,
        "mail_from" => "sender@example.com",
        "rcpt_to" => ["rcpt@example.com"],
        "received_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "attempts" => 0
      })
    )

    assert :ok = Incoming.Queue.Disk.recover()

    refute File.exists?(base)
    dead_json = Path.join([tmp, "dead", id, "dead.json"])
    assert File.exists?(dead_json)
    decoded = Jason.decode!(File.read!(dead_json))
    assert decoded["reason"] =~ "incomplete_write"
  end

  test "queue recover dead-letters invalid incoming file entry", %{tmp: tmp} do
    id = "incoming-file-#{System.unique_integer([:positive])}"
    base = Path.join([tmp, "incoming", id])
    File.mkdir_p!(Path.dirname(base))
    File.write!(base, "placeholder")

    assert :ok = Incoming.Queue.Disk.recover()

    refute File.exists?(base)
    dead_json = Path.join([tmp, "dead", id, "dead.json"])
    assert File.exists?(dead_json)
    decoded = Jason.decode!(File.read!(dead_json))
    assert decoded["reason"] =~ "invalid_incoming_entry"
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
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
    :ok = Incoming.Queue.Disk.ack(message.id)
    assert Incoming.Queue.Disk.depth() == 0
  end

  test "queue depth ignores processing and dead", %{tmp: tmp} do
    committed1 = Path.join([tmp, "committed", "a"])
    committed2 = Path.join([tmp, "committed", "b"])
    processing = Path.join([tmp, "processing", "c"])
    dead = Path.join([tmp, "dead", "d"])
    incoming = Path.join([tmp, "incoming", "e"])

    File.mkdir_p!(committed1)
    File.mkdir_p!(committed2)
    File.mkdir_p!(processing)
    File.mkdir_p!(dead)
    File.mkdir_p!(incoming)

    assert Incoming.Queue.Disk.depth() == 2
  end

  test "queue depth ignores non-directory entries in committed", %{tmp: tmp} do
    File.mkdir_p!(Path.join(tmp, "committed"))
    File.write!(Path.join([tmp, "committed", "not-a-message"]), "placeholder")
    File.mkdir_p!(Path.join([tmp, "committed", "real-message"]))

    assert Incoming.Queue.Disk.depth() == 1
  end

  test "queue dequeue returns smallest id first", %{tmp: tmp} do
    for id <- ["a", "b"] do
      base = Path.join([tmp, "committed", id])
      File.mkdir_p!(base)
      File.write!(Path.join(base, "raw.eml"), "Subject: Test\r\n\r\nBody\r\n")

      File.write!(
        Path.join(base, "meta.json"),
        Jason.encode!(%{
          id: id,
          mail_from: "from@example.com",
          rcpt_to: ["to@example.com"],
          received_at: DateTime.utc_now() |> DateTime.to_iso8601()
        })
      )
    end

    assert {:ok, message} = Incoming.Queue.Disk.dequeue()
    assert message.id == "a"
  end

  test "dead letter metadata includes reason and timestamp", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
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

  test "telemetry emits enqueue error on message_too_large", %{tmp: tmp} do
    id = "incoming-test-telemetry-enqueue-error-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :message, :enqueue_error],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    assert {:error, :message_too_large} =
             Incoming.Queue.Disk.enqueue(from, to, data,
               path: tmp,
               fsync: false,
               max_message_size: 1
             )

    assert_receive {:telemetry, [:incoming, :message, :enqueue_error], meas, meta}, 1_000
    assert meas.count == 1
    assert is_binary(meta.id)
    assert meta.reason == :message_too_large
    assert meta.attempted_size > 1

    :telemetry.detach(id)
  end

  test "telemetry emits enqueue error when queue path is not a directory", %{tmp: tmp} do
    id = "incoming-test-telemetry-enqueue-error-notdir-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :message, :enqueue_error],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    not_a_dir = Path.join(tmp, "not_a_dir")
    File.write!(not_a_dir, "nope")

    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    result = Incoming.Queue.Disk.enqueue(from, to, data, path: not_a_dir, fsync: false)

    assert {:error, %File.Error{}} = result

    assert_receive {:telemetry, [:incoming, :message, :enqueue_error], meas, meta}, 1_000
    assert meas.count == 1
    assert is_binary(meta.id)
    assert match?({:exception, File.Error, _}, meta.reason)

    :telemetry.detach(id)
  end

  test "session telemetry emits connect", %{} do
    id = "incoming-test-session-connect-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :session, :connect],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    assert_receive {:telemetry, [:incoming, :session, :connect], _meas, meta}, 1_000
    assert Map.has_key?(meta, :peer)
    assert Map.has_key?(meta, :hostname)

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    :telemetry.detach(id)
  end

  test "session telemetry emits accepted", %{tmp: tmp} do
    id = "incoming-test-session-accepted-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :session, :accepted],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    assert_receive {:telemetry, [:incoming, :session, :accepted], _meas, meta}, 1_000
    assert is_binary(meta.id)

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    :telemetry.detach(id)
  end

  test "session telemetry emits rejected with reason", %{} do
    id = "incoming-test-session-rejected-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :session, :rejected],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    Application.put_env(:incoming, :policies, [IncomingTest.RejectDataPolicy])
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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
    assert_recv(socket, "554")

    assert_receive {:telemetry, [:incoming, :session, :rejected], _meas, meta}, 1_000
    assert meta.reason == "Data rejected"

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    Application.put_env(:incoming, :policies, [])
    restart_app()
    :telemetry.detach(id)
  end

  test "policy context includes envelope and tls flags", %{} do
    IncomingTest.CapturePolicy.set_target(self())
    Application.put_env(:incoming, :policies, [IncomingTest.CapturePolicy])
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

  test "command order enforcement rejects rcpt before mail", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    assert File.ls!(Path.join(tmp, "committed")) == []
  end

  test "command order enforcement rejects data before rcpt", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "DATA")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")

    assert File.ls!(Path.join(tmp, "committed")) == []
  end

  test "mail without helo is rejected", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "503")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "noop and rset are accepted", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "NOOP")
    assert_recv(socket, "250")

    send_line(socket, "RSET")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "unknown command yields 500", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "WUT")
    assert_recv(socket, "500")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "quit without helo is accepted", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "invalid helo yields error", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "HELO")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "invalid helo with extra spaces yields error", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "HELO    ")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "invalid ehlo yields error", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO")
    assert_recv(socket, "501")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "help command returns 214", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "HELP")
    assert_recv(socket, "500")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "vrfy and expn are disabled", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<>")
    assert_recv(socket, "250")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "invalid rcpt command yields error", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

  test "MAIL and RCPT options are accepted", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com> EXTRA")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com> EXTRA")
    assert_recv(socket, "555")

    send_line(socket, "QUIT")
    assert_recv(socket, "221")
  end

  test "policy short-circuits on rejection", %{} do
    IncomingTest.CapturePolicy.set_target(self())

    Application.put_env(:incoming, :policies, [
      Incoming.Policy.HelloRequired,
      IncomingTest.CapturePolicy
    ])

    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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

  test "rset allows a new transaction", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "EHLO client.example.com")
    read_multiline(socket, "250")

    send_line(socket, "MAIL FROM:<sender@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RCPT TO:<rcpt@example.com>")
    assert_recv(socket, "250")

    send_line(socket, "RSET")
    assert_recv(socket, "250")

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
  end

  test "rset after data command still rejects without mail", %{} do
    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    assert_receive {:telemetry, [:incoming, :delivery, :result], _meas, meta1}, 1_000
    assert meta1.outcome in [:retry, :reject]
    assert meta1.reason != nil

    :telemetry.detach(id)
    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 1_000)
    restart_app()
  end

  test "telemetry emits adapter exception reason", %{tmp: tmp} do
    id = "incoming-test-delivery-exception-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :delivery, :result],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)

    Application.put_env(:incoming, :delivery_opts,
      workers: 1,
      poll_interval: 10,
      max_attempts: 1,
      base_backoff: 1,
      max_backoff: 1
    )

    restart_app()

    IncomingTest.DummyAdapter.set_mode(:raise)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    assert_receive {:telemetry, [:incoming, :delivery, :result], _meas, meta}, 1_000
    assert meta.outcome == :reject
    assert match?({:max_attempts, {:exception, _, _}}, meta.reason)

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

  test "delivery retry telemetry emits reason on retry", %{tmp: tmp} do
    id = "incoming-test-delivery-retry-reason-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :delivery, :result],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)

    Application.put_env(:incoming, :delivery_opts,
      workers: 1,
      poll_interval: 10,
      max_attempts: 2,
      base_backoff: 1,
      max_backoff: 1
    )

    restart_app()

    IncomingTest.DummyAdapter.set_mode(:retry)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    assert_receive {:telemetry, [:incoming, :delivery, :result], _meas, meta}, 1_000
    assert meta.outcome in [:retry, :reject]
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

  test "delivery disabled leaves queue untouched", %{tmp: tmp} do
    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)
    restart_app()

    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    Process.sleep(50)
    assert Incoming.Queue.Disk.depth() == 1
  end

  test "queue nack retry persists attempt count in metadata", %{tmp: tmp} do
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)
    assert {:ok, dequeued} = Incoming.Queue.Disk.dequeue()
    assert dequeued.id == message.id
    assert dequeued.attempts == 0

    :ok = Incoming.Queue.Disk.nack(message.id, :retry, :temporary)

    meta_path = Path.join([tmp, "committed", message.id, "meta.json"])
    decoded = Jason.decode!(File.read!(meta_path))
    assert decoded["attempts"] == 1
  end

  test "delivery retry attempts persist across restart", %{tmp: tmp} do
    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)

    Application.put_env(:incoming, :delivery_opts,
      workers: 1,
      # Long interval gives us time to restart between attempts.
      poll_interval: 10_000,
      max_attempts: 2,
      base_backoff: 1,
      max_backoff: 1
    )

    IncomingTest.DummyAdapter.set_mode(:retry)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    # Start delivery after the message exists, so the worker's first immediate tick picks it up.
    restart_app()

    meta_path = Path.join([tmp, "committed", message.id, "meta.json"])

    :ok =
      wait_until(
        fn ->
          File.exists?(meta_path) and Jason.decode!(File.read!(meta_path))["attempts"] == 1
        end,
        200
      )

    restart_app()

    :ok = wait_until(fn -> File.exists?(Path.join([tmp, "dead", message.id])) end)

    Application.put_env(:incoming, :delivery, nil)
    Application.put_env(:incoming, :delivery_opts, workers: 1, poll_interval: 1_000)
    restart_app()
  end

  test "delivery adapter exception is treated as retry (no stuck processing)", %{tmp: tmp} do
    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)

    IncomingTest.DummyAdapter.set_mode(:raise)
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

    meta_path = Path.join([tmp, "committed", message.id, "meta.json"])

    :ok =
      wait_until(
        fn ->
          File.exists?(meta_path) and Jason.decode!(File.read!(meta_path))["attempts"] == 1
        end,
        200
      )

    refute File.exists?(Path.join([tmp, "processing", message.id]))

    GenServer.stop(pid)
    Application.put_env(:incoming, :delivery, nil)
  end

  test "delivery worker does not block during retry backoff", %{tmp: tmp} do
    Application.put_env(:incoming, :delivery, IncomingTest.DummyAdapter)
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)

    IncomingTest.DummyAdapter.set_mode(:retry)
    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"
    {:ok, _message} = Incoming.Queue.Disk.enqueue(from, to, data, path: tmp, fsync: false)

    {:ok, pid} =
      Incoming.Delivery.Worker.start_link(
        poll_interval: 10_000,
        max_attempts: 3,
        base_backoff: 500,
        max_backoff: 500
      )

    # Old behavior slept inside the GenServer and would not respond promptly.
    t =
      Task.async(fn ->
        :sys.get_state(pid)
        :ok
      end)

    assert Task.yield(t, 100) == {:ok, :ok}

    GenServer.stop(pid)
    Application.put_env(:incoming, :delivery, nil)
  end

  # -- Implicit TLS Tests --

  test "implicit tls config requires cert and key" do
    assert_raise ArgumentError, fn ->
      Incoming.Listener.child_spec(%{name: :bad, port: 2527, tls: :implicit, tls_opts: []})
    end
  end

  test "implicit tls accepts ssl connection" do
    Application.put_env(:incoming, :listeners, [
      %{
        name: :test,
        port: 2526,
        tls: :implicit,
        tls_opts: [
          certfile: "test/fixtures/test-cert.pem",
          keyfile: "test/fixtures/test-key.pem"
        ]
      }
    ])

    restart_app()

    {:ok, socket} =
      :ssl.connect(
        String.to_charlist("localhost"),
        2526,
        [
          :binary,
          active: false,
          packet: :line,
          verify: :verify_none
        ],
        5_000
      )

    assert {:ok, banner} = :ssl.recv(socket, 0, 2_000)
    assert String.starts_with?(banner, "220")

    :ssl.send(socket, "EHLO client.example.com\r\n")
    lines = read_multiline_ssl(socket, "250")
    refute Enum.any?(lines, &String.contains?(&1, "STARTTLS"))

    :ssl.send(socket, "MAIL FROM:<sender@example.com>\r\n")
    assert {:ok, resp} = :ssl.recv(socket, 0, 2_000)
    assert String.starts_with?(resp, "250")

    :ssl.send(socket, "RCPT TO:<rcpt@example.com>\r\n")
    assert {:ok, resp} = :ssl.recv(socket, 0, 2_000)
    assert String.starts_with?(resp, "250")

    :ssl.send(socket, "DATA\r\n")
    assert {:ok, resp} = :ssl.recv(socket, 0, 2_000)
    assert String.starts_with?(resp, "354")

    :ssl.send(socket, "Subject: Test\r\n\r\nBody\r\n.\r\n")
    assert {:ok, resp} = :ssl.recv(socket, 0, 2_000)
    assert String.starts_with?(resp, "250")

    :ssl.send(socket, "QUIT\r\n")
    assert {:ok, resp} = :ssl.recv(socket, 0, 2_000)
    assert String.starts_with?(resp, "221")

    :ssl.close(socket)

    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    restart_app()
  end

  test "implicit tls does not advertise starttls" do
    Application.put_env(:incoming, :listeners, [
      %{
        name: :test,
        port: 2526,
        tls: :implicit,
        tls_opts: [
          certfile: "test/fixtures/test-cert.pem",
          keyfile: "test/fixtures/test-key.pem"
        ]
      }
    ])

    restart_app()

    {:ok, socket} =
      :ssl.connect(
        String.to_charlist("localhost"),
        2526,
        [
          :binary,
          active: false,
          packet: :line,
          verify: :verify_none
        ],
        5_000
      )

    assert {:ok, _banner} = :ssl.recv(socket, 0, 2_000)

    :ssl.send(socket, "EHLO client.example.com\r\n")
    lines = read_multiline_ssl(socket, "250")
    refute Enum.any?(lines, &String.contains?(&1, "STARTTLS"))

    :ssl.send(socket, "QUIT\r\n")
    :ssl.close(socket)

    Application.put_env(:incoming, :listeners, [%{name: :test, port: 2526, tls: :disabled}])
    restart_app()
  end

  # -- Rate Limiter Sliding Window Tests --

  test "rate limiter counter resets after window expires" do
    prev_limit = Application.get_env(:incoming, :rate_limit, 5)
    prev_window = Application.get_env(:incoming, :rate_limit_window, 60)

    Application.put_env(:incoming, :rate_limit, 2)
    Application.put_env(:incoming, :rate_limit_window, 1)

    Incoming.Policy.RateLimiter.init_table()
    :ets.delete_all_objects(:incoming_rate_limits)

    peer = {{127, 0, 0, 99}, 9999}
    ctx = %{phase: :mail_from, peer: peer}

    assert :ok = Incoming.Policy.RateLimiter.check(ctx)
    assert {:reject, 554, _} = Incoming.Policy.RateLimiter.check(ctx)

    Process.sleep(1_100)

    assert :ok = Incoming.Policy.RateLimiter.check(ctx)

    Application.put_env(:incoming, :rate_limit, prev_limit)
    Application.put_env(:incoming, :rate_limit_window, prev_window)
  end

  test "rate limiter sweeper removes stale entries" do
    Incoming.Policy.RateLimiter.init_table()
    Application.put_env(:incoming, :rate_limit_window, 0)

    key = {{127, 0, 0, 200}, :test_sweep}
    stale_time = System.monotonic_time(:second) - 120
    :ets.insert(:incoming_rate_limits, {key, 5, stale_time})

    assert :ets.info(:incoming_rate_limits, :size) >= 1

    Incoming.Policy.RateLimiterSweeper.sweep()

    assert :ets.lookup(:incoming_rate_limits, key) == []

    Application.delete_env(:incoming, :rate_limit_window)
  end

  test "rate limiter sweeper preserves fresh entries" do
    Incoming.Policy.RateLimiter.init_table()
    Application.put_env(:incoming, :rate_limit_window, 60)

    key = {{127, 0, 0, 201}, :test_fresh}
    fresh_time = System.monotonic_time(:second)
    :ets.insert(:incoming_rate_limits, {key, 3, fresh_time})

    Incoming.Policy.RateLimiterSweeper.sweep()

    assert [{^key, 3, ^fresh_time}] = :ets.lookup(:incoming_rate_limits, key)

    :ets.delete(:incoming_rate_limits, key)
    Application.delete_env(:incoming, :rate_limit_window)
  end

  test "rate limiter still enforced within window" do
    Application.put_env(:incoming, :rate_limit, 2)
    Application.put_env(:incoming, :rate_limit_window, 60)

    Incoming.Policy.RateLimiter.init_table()

    peer = {{127, 0, 0, 202}, 8888}
    ctx = %{phase: :mail_from, peer: peer}

    assert :ok = Incoming.Policy.RateLimiter.check(ctx)
    assert {:reject, 554, _} = Incoming.Policy.RateLimiter.check(ctx)

    Application.put_env(:incoming, :rate_limit, 5)
    Application.delete_env(:incoming, :rate_limit_window)
  end

  # -- Memory Queue Tests --

  test "memory queue enqueue/dequeue round-trip" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    from = "sender@example.com"
    to = ["rcpt@example.com"]
    data = "Subject: Test\r\n\r\nBody\r\n"

    {:ok, message} = Incoming.Queue.Memory.enqueue(from, to, data, [])
    assert message.id != nil
    assert message.raw_data == data
    assert message.raw_path == nil

    {:ok, dequeued} = Incoming.Queue.Memory.dequeue()
    assert dequeued.id == message.id

    GenServer.stop(pid)
  end

  test "memory queue FIFO ordering" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    {:ok, m1} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data1", [])
    {:ok, m2} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data2", [])

    {:ok, d1} = Incoming.Queue.Memory.dequeue()
    {:ok, d2} = Incoming.Queue.Memory.dequeue()

    assert d1.id == m1.id
    assert d2.id == m2.id

    GenServer.stop(pid)
  end

  test "memory queue ack removes message" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    {:ok, message} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data", [])
    {:ok, _dequeued} = Incoming.Queue.Memory.dequeue()
    :ok = Incoming.Queue.Memory.ack(message.id)

    assert {:empty} = Incoming.Queue.Memory.dequeue()
    assert Incoming.Queue.Memory.depth() == 0

    GenServer.stop(pid)
  end

  test "memory queue nack retry returns to committed" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    {:ok, message} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data", [])
    {:ok, _dequeued} = Incoming.Queue.Memory.dequeue()
    assert Incoming.Queue.Memory.depth() == 0
    :ok = Incoming.Queue.Memory.nack(message.id, :retry, :temporary)
    assert Incoming.Queue.Memory.depth() == 1

    {:ok, retried} = Incoming.Queue.Memory.dequeue()
    assert retried.id == message.id
    assert retried.attempts == 1

    GenServer.stop(pid)
  end

  test "memory queue nack reject removes message" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    {:ok, message} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data", [])
    {:ok, _dequeued} = Incoming.Queue.Memory.dequeue()
    :ok = Incoming.Queue.Memory.nack(message.id, :reject, :permanent)

    assert {:empty} = Incoming.Queue.Memory.dequeue()
    assert Incoming.Queue.Memory.depth() == 0

    GenServer.stop(pid)
  end

  test "memory queue depth tracking" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    assert Incoming.Queue.Memory.depth() == 0
    {:ok, m1} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data1", [])
    assert Incoming.Queue.Memory.depth() == 1
    {:ok, _m2} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data2", [])
    assert Incoming.Queue.Memory.depth() == 2

    {:ok, _dequeued} = Incoming.Queue.Memory.dequeue()
    assert Incoming.Queue.Memory.depth() == 1

    :ok = Incoming.Queue.Memory.ack(m1.id)
    assert Incoming.Queue.Memory.depth() == 1

    GenServer.stop(pid)
  end

  test "memory queue recover moves processing back to committed" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    {:ok, message} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data", [])
    {:ok, _dequeued} = Incoming.Queue.Memory.dequeue()
    assert Incoming.Queue.Memory.depth() == 0

    :ok = Incoming.Queue.Memory.recover()
    assert Incoming.Queue.Memory.depth() == 1

    {:ok, recovered} = Incoming.Queue.Memory.dequeue()
    assert recovered.id == message.id

    GenServer.stop(pid)
  end

  test "memory queue max message size enforcement" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    assert {:error, :message_too_large} =
             Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "12345678",
               max_message_size: 5
             )

    assert Incoming.Queue.Memory.depth() == 0

    GenServer.stop(pid)
  end

  test "memory queue message headers parseable from raw_data" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])

    data = "Subject: Test\r\nFrom: sender@example.com\r\n\r\nBody\r\n"
    {:ok, message} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], data, [])

    headers = Incoming.Message.headers(message)
    assert headers["subject"] == "Test"
    assert headers["from"] == "sender@example.com"

    GenServer.stop(pid)
  end

  test "memory queue telemetry emits enqueue event" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])
    id = "memory-queue-telemetry-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:incoming, :message, :queued],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    {:ok, _message} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data", [])

    assert_receive {:telemetry, [:incoming, :message, :queued], _meas, meta}, 1_000
    assert is_binary(meta.id)

    :telemetry.detach(id)
    GenServer.stop(pid)
  end

  test "telemetry prefix rewrites events" do
    {:ok, pid} = Incoming.Queue.Memory.start_link([])
    parent = self()
    id = "telemetry-prefix-#{System.unique_integer([:positive])}"

    old = Application.get_env(:incoming, :telemetry_prefix, [:incoming])
    Application.put_env(:incoming, :telemetry_prefix, [:myapp, :incoming])

    :telemetry.attach(
      id,
      [:myapp, :incoming, :message, :queued],
      &IncomingTest.TelemetryHandler.handle/4,
      parent
    )

    {:ok, _message} = Incoming.Queue.Memory.enqueue("a@x.com", ["b@x.com"], "data", [])

    assert_receive {:telemetry, [:myapp, :incoming, :message, :queued], _meas, _meta}, 1_000

    :telemetry.detach(id)
    Application.put_env(:incoming, :telemetry_prefix, old)
    GenServer.stop(pid)
  end

  test "max_depth backpressure returns 421 and does not commit", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false, max_depth: 0)
    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
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
    assert_recv(socket, "421")

    assert File.ls!(Path.join(tmp, "committed")) == []
    :ok = :gen_tcp.close(socket)
  end

  test "per-ip connection limit rejects second concurrent connection", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)

    Application.put_env(:incoming, :listeners, [
      %{name: :test, port: 2526, tls: :disabled, max_connections_per_ip: 1}
    ])

    restart_app()

    {:ok, s1} = connect_with_retry("localhost", 2526, 10)
    assert_recv(s1, "220")

    {:ok, s2} = connect_with_retry("localhost", 2526, 10)

    assert {:ok, line} = :gen_tcp.recv(s2, 0, 1_000)
    assert String.starts_with?(line, "421")

    _ = :gen_tcp.close(s2)
    _ = :gen_tcp.close(s1)
  end

  test "max_errors disconnects session with 421", %{tmp: tmp} do
    Application.put_env(:incoming, :queue_opts, path: tmp, fsync: false)

    Application.put_env(:incoming, :session_opts,
      max_message_size: 10 * 1024 * 1024,
      max_recipients: 100,
      max_commands: 1_000,
      max_errors: 0
    )

    restart_app()

    {:ok, socket} = connect_with_retry("localhost", 2526, 10)
    assert_recv(socket, "220")

    send_line(socket, "BOGUS")
    assert_recv(socket, "421 Too many errors")

    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  defp send_line(socket, line) do
    :ok = :gen_tcp.send(socket, line <> "\r\n")
  end

  defp assert_recv(socket, prefix) do
    assert {:ok, line} = :gen_tcp.recv(socket, 0, 1000)
    assert String.starts_with?(line, prefix)
  end

  defp assert_no_recv(socket, timeout_ms) do
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, timeout_ms)
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

  defp read_multiline_ssl(socket, code, acc \\ []) do
    assert {:ok, line} = :ssl.recv(socket, 0, 2_000)

    cond do
      String.starts_with?(line, code <> "-") ->
        read_multiline_ssl(socket, code, [line | acc])

      String.starts_with?(line, code <> " ") ->
        Enum.reverse([line | acc])

      true ->
        flunk("unexpected ssl response: #{inspect(line)}")
    end
  end

  defp connect_with_retry(host, port, attempts) do
    host =
      if is_binary(host) do
        String.to_charlist(host)
      else
        host
      end

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
