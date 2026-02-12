defmodule Incoming.MixProject do
  use Mix.Project

  def project do
    [
      app: :incoming,
      version: "0.5.0",
      name: "Incoming",
      description: description(),
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Incoming.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:gen_smtp, "~> 1.2"},
      {:telemetry, "~> 1.2"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp description do
    "Production-grade inbound SMTP server library for Elixir."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/pgeraghty/incoming"}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "guide.md", "planned_improvements_plan.md"],
      source_ref: "main"
    ]
  end
end
