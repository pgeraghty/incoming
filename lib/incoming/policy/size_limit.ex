defmodule Incoming.Policy.SizeLimit do
  @moduledoc false

  @behaviour Incoming.Policy

  @impl true
  def check(%{phase: :data_start, max_message_size: max}) when is_integer(max) do
    if max <= 0 do
      {:reject, 552, "Message too large"}
    else
      :ok
    end
  end

  def check(_context), do: :ok
end
