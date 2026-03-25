package config

class DatalakeConfig{
    var rawDb : String = "raw";
    var rawTable : String = "raw_table";
    var catalogName : String = "hudi"
    var silverDb : String = "silver";
    var silverTransactionTable : String = "fact_transaction";
    var silverCustomerTable : String = "dim_customer";
    var dimProduct : String = "dim_product";
}