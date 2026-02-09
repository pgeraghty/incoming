defmodule Incoming.Policy.MaxRecipients do
  @moduledoc false

  @behaviour Incoming.Policy

  @impl true
  def check(%{phase: :rcpt_to, envelope: %{rcpt_to: rcpt_to}, max_recipients: max})
      when is_integer(max) do
    if length(rcpt_to) > max do
      {:reject, 452, "Too many recipients"}
    else
      :ok
    end
  end

  def check(_context), do: :ok
end
