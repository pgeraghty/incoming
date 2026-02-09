defmodule Incoming.Policy.TlsRequired do
  @moduledoc false

  @behaviour Incoming.Policy

  @impl true
  def check(%{phase: phase, tls_mode: :required, tls_active: false})
      when phase in [:mail_from, :rcpt_to, :data_start] do
    {:reject, 530, "Must issue STARTTLS first"}
  end

  def check(_context), do: :ok
end
