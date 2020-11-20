from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest
from wasmaster.wasproductsource import WasProduct
from wasmaster.wasmastertarget import WasMasterTarget


@pytest.fixture
def product_master_source(utility):
    product_source = TbToValidate(WasProduct(), utils=utility).get_table(WasProduct.was_product)
    return product_source


@pytest.fixture
def product_master_target(redshift_db_connection, utility):
    product_target = TbToValidate(WasMasterTarget(), sql="""SELECT id,_doctype,author,isbn from public.was_product
                WHERE year='{}' AND month='{}' AND DAY='{}'""".format(utility.year,
                                                                      utility.month,
                                                                      utility.day), dbconnection=redshift_db_connection, chunksize=0).get_table(WasMasterTarget.pandaDf)
    return product_target


@pytest.mark.dependency(name="main-test_cp")
@pytest.mark.run(order=1)
def test_source_json_general(product_master_source):

    result = Validation().run_validation_on(product_master_source).expects_column_to_exist("id", "presence Check column-Id").\
                                                                      expect_table_row_count_grater_than(0, "raw count validation-source").\
                                                                      get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=2)
def test_source_target(product_master_source,product_master_target):
    colums_to_test = ["id", "_doctype", "author", "isbn"]

    result = Validation().expect_table1_all_rows_overlap_with_table2(product_master_source, product_master_target, colums_to_test,"test_cp source and target table amaster").get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'
