package etl.mifos.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class ClearDataFromDataBase {

    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String[] QUERY = {
        "truncate table fact_savings_transactions",
        "truncate table fact_loan_repayments",
        "truncate table fact_loan_disbursals",
        "truncate table fact_customer_fees_and_charges",
        "truncate table fact_loan_fees_and_charges",
        "truncate table dw_loan_schedules",
        "truncate table dw_loan_fee_schedules",
        "truncate table dw_customer_schedules",
        "truncate table dw_customer_fee_schedules",
        "truncate table fact_client_attendances",
        "truncate table dw_ppi_survey",
        "truncate table fact_ppi_survey_results",
        "truncate table hist_savings_balances",
        "truncate table hist_loan_balances",
        "truncate table hist_loan_arrears",
        "truncate table stg_personnel_names_and_name_changes",
        "truncate table stg_customer_and_account_updates",
        "truncate table stg_customer_type1_columns",
        "truncate table stg_loan_type1_columns",
        "delete from dw_account_action where account_action_id > 0",
        "delete from dw_currency where currency_id > 0",
        "delete from dw_fee where fee_id > 0",
        "delete from dw_fund where fund_id > 0",
        "delete from dim_product where product_key > 0",
        "delete from dim_office where office_key > 0",
        "delete from dim_personnel where personnel_key > 0",
        "delete from dim_customer where customer_key > 0",
        "delete from dim_loan where loan_account_key > 0",
        "delete from dim_savings where savings_account_key > 0"        
    };
    public static Connection getConnection(String user, String password, String url) throws Exception {
        Class.forName(DRIVER).newInstance();
        return DriverManager.getConnection(url, user, password);
    }

    public void resetDatabase(String user, String password, String url) throws Exception {
        Connection connection = ClearDataFromDataBase.getConnection(user, password, url);
        Statement statement = connection.createStatement();
        for (int i = 0; i < QUERY.length; i++) {
            if (!QUERY[i].trim().equals("")) {
                statement.executeUpdate(QUERY[i]);
            }
        }
        statement.close();
        connection.close();
    }
}