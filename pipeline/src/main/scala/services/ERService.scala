package services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class ERService (neo4jService: Neo4jService, spark: SparkSession){
    def getAllNode(): DataFrame = {
       var query = s"""
            MATCH (n:User)
            OPTIONAL MATCH (n)-[r]->(m)
            RETURN 
                id(n) AS user_internal_id, 
                n.id AS customer_id,
                labels(m)[0] AS entity_type, 
                [k in keys(m) | m[k]][0] AS attrs
        """
        return neo4jService.queryDatabase(query);
    }

    def writeCustomerGraph(customerDf: DataFrame): Unit = {
        var query: String = """
            MERGE (c:Customer {id: event.customer_id})
            FOREACH (ignoreMe IN CASE WHEN event.email IS NOT NULL THEN [1] ELSE [] END |
                MERGE (e:Email {val: event.email})
                MERGE (c)-[:HAS_EMAIL]->(e)
            )
            FOREACH (ignoreMe IN CASE WHEN event.phone_number IS NOT NULL THEN [1] ELSE [] END |
                MERGE (e:PhoneNumber {val: event.phone_number})
                MERGE (c)-[:HAS_PHONE_NUMBER]->(e)
            )
            FOREACH (ignoreMe IN CASE WHEN event.client_id IS NOT NULL THEN [1] ELSE [] END |
                MERGE (e:ClientID {val: event.client_id})
                MERGE (c)-[:HAS_CLIENT_ID]->(e)
            )
        """
        neo4jService.writeDf(customerDf, query);
    }
}
