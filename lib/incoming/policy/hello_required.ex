defmodule Incoming.Policy.HelloRequired do
  @moduledoc false

  @behaviour Incoming.Policy

  @impl true
  def check(%{phase: phase, seen_helo: false})
      when phase in [:mail_from, :rcpt_to, :data_start] do
    {:reject, 503, "Send HELO/EHLO first"}
  end

  def check(_context), do: :ok
end
