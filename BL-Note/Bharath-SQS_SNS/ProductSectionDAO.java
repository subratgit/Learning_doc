package nz.com.reece.product.dao;

import lombok.extern.slf4j.Slf4j;
import nz.com.reece.product.model.ProductMasterData;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;


@Component
@Slf4j
public class ProductSectionDAO {


    private static final String RETRY_MAX_ATTEMPTS = "${productMasterData.sql.retry.maxAttempts}";
    private static final String RETRY_MAX_DELAY = "${productMasterData.sql.retry.maxDelayMs}";
    private static final String LOOKUP_TABLE_CODE = "CA";

    private final JdbcTemplate jdbcTemplate;


    private static final String INSERT_QUERY = "INSERT INTO oldlukup " +
            "(lutabid, lucode, ludesc, luparam) " +
            "VALUES(?, ?, ?, ?)";

    private static final String UPDATE_QUERY = "UPDATE oldlukup " +
            "SET ludesc = ? " +
            "WHERE lucode = ? AND lutabid = ?";

    private static final String DELETE_QUERY = "DELETE FROM oldlukup " +
            "WHERE lucode = ? AND lutabid = ?";


    public ProductSectionDAO(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }



    @Retryable(value = DataAccessException.class, maxAttemptsExpression = RETRY_MAX_ATTEMPTS,
            backoff = @Backoff(delayExpression = RETRY_MAX_DELAY))
    public void insert(ProductMasterData productMaster) {

        jdbcTemplate.update(con -> {
            PreparedStatement preparedStatement = con.prepareStatement(INSERT_QUERY);
            preparedStatement.setString(1, LOOKUP_TABLE_CODE);
            preparedStatement.setString(2, productMaster.getCode());
            preparedStatement.setString(3, productMaster.getDescription());
            preparedStatement.setLong(4, 0);
            return preparedStatement;
        });

    }

    @Retryable(value = DataAccessException.class, maxAttemptsExpression = RETRY_MAX_ATTEMPTS,
            backoff = @Backoff(delayExpression = RETRY_MAX_DELAY))
    public void update(ProductMasterData productMaster) {

        jdbcTemplate.update(con -> {
            PreparedStatement preparedStatement = con.prepareStatement(UPDATE_QUERY);
            preparedStatement.setString(1, productMaster.getDescription());
            preparedStatement.setString(2, productMaster.getCode());
            preparedStatement.setString(3, LOOKUP_TABLE_CODE);
            return preparedStatement;
        });

    }


    @Retryable(value = DataAccessException.class, maxAttemptsExpression = RETRY_MAX_ATTEMPTS,
            backoff = @Backoff(delayExpression = RETRY_MAX_DELAY))
    public void delete(String prodMasterCode) {

        jdbcTemplate.update(con -> {
            PreparedStatement preparedStatement = con.prepareStatement(DELETE_QUERY);
            preparedStatement.setString(1, prodMasterCode);
            preparedStatement.setString(2, LOOKUP_TABLE_CODE);
            return preparedStatement;
        });

    }

}
