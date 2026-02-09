defmodule Incoming.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(_args) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    queue_child = {Incoming.Config.queue_module(), Incoming.Config.queue_opts()}

    listener_children =
      Incoming.Config.listeners()
      |> Enum.map(&Incoming.Listener.child_spec/1)

    children = [queue_child | listener_children]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
