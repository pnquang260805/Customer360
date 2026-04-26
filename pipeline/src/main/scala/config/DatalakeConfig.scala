package config

class DatalakeConfig {
  var rawDb: String = "raw";
  var rawTable: String = "raw_table";
  var catalogName: String = "hudi"
  var silverDb: String = "silver";
  var goldDb: String = "gold";

  // Silver layer (cleansed)
  var silverTransactionTable: String = "silver_transaction";
  var silverCustomerTable: String = "silver_customer";
  var silverProductTable: String = "silver_product";

  // Gold layer (star schema)
  var dimCustomer: String = "dim_customer";
  var dimProduct: String = "dim_product";
  var dimDate: String = "dim_date";
  var factTransaction: String = "fact_transaction";
}
