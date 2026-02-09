defmodule Incoming.Delivery.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(_args) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    adapter = Incoming.Config.delivery_adapter()
    opts = Incoming.Config.delivery_opts()
    workers = Keyword.get(opts, :workers, 1)

    children =
      if adapter do
        Enum.map(1..workers, fn idx ->
          %{
            id: {Incoming.Delivery.Worker, idx},
            start: {Incoming.Delivery.Worker, :start_link, [opts]}
          }
        end)
      else
        []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
