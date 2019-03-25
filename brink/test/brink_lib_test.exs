defmodule BrinkTest.Lib do
  use ExUnit.Case
  use PropCheck
  #import Mock
  doctest Brink.Lib

  def event(), do: [utf8(), [utf8(), utf8()]]
end
