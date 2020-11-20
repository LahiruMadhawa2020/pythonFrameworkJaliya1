from great_expectations.dataset import (
    PandasDataset,
    MetaPandasDataset
)


class CustomedRules(PandasDataset):

    @MetaPandasDataset.column_map_expectation
    def __expect_column_values_to_be_less_than(self, column, value):
        return column.map(lambda x: x < value)