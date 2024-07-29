def test_most_expensive_item_quantity():
  # Create a sample DataFrame for testing
  test_data = [
      ("item1", 1, 10.0),
      ("item2", 2, 15.0),
      ("item3", 3, 12.0)
  ]
  test_df = spark.createDataFrame(test_data, ["item_name", "quantity", "item_price"])

  # Call the function
  result = most_expensive_item_quantity(test_df)

  # Assert the expected result
  assert result == 2, f"Expected quantity 2, but got {result}"

# Run the test case
test_most_expensive_item_quantity()
print("Test passed!")