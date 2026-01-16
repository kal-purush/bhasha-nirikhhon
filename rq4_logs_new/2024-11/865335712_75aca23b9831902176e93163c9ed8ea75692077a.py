import numpy as np
from systemds.context import SystemDSContext
from systemds.operator.algorithm import lm
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error
import time
import shutil

from system_logging import SystemLogging

def run_systemds_lr(system_logging):
    with SystemDSContext() as sds:
        starttime = time.time()
        endtime = 0

        try:
            # Load data into numpy arrays
            data = np.genfromtxt("houring_prices.csv", delimiter=",", skip_header=1)

            X = data[:, :-1]
            y = data[:, -1]

            loop_start = time.time()
            while loop_start + 15 > time.time():
                # Perform the train-test split (80/20)
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

                # Convert to SystemDS matrices
                X_train_sds = sds.from_numpy(X_train)
                y_train_sds = sds.from_numpy(y_train.reshape(-1, 1))
                X_test_sds = sds.from_numpy(X_test)
                # y_test_sds = sds.from_numpy(y_test.reshape(-1, 1))

                # Train the linear regression model
                model = lm(X_train_sds, y_train_sds)

                # Make predictions on the test set
                predictions = (X_test_sds @ model).compute()

                # Calculate metrics
                errors = predictions - y_test
                mse = np.mean(errors ** 2)
                rmse = np.sqrt(mse)
                mae = mean_absolute_error(y_test, predictions)
                r2 = r2_score(y_test, predictions)

                if endtime == 0:
                    endtime = time.time()

            system_logging.write_output(f"Mean Squared Error (MSE): {mse:.4f}")
            system_logging.write_output(f"Root Mean Squared Error (RMSE): {rmse:.4f}")
            system_logging.write_output(f"[Mean Absolute Error (MAE): {mae:.4f}")
            system_logging.write_output(f"R² (Coefficient of Determination): {r2:.4f}")

        except Exception as e:
            system_logging.write_output(f"Error: {str(e)}")
        
        finally:
            exec_time = endtime - starttime
            system_logging.write_output(f"Execution Time: {exec_time:.2f} seconds")


if __name__ == "__main__":
    ml_system = "systemds"

    systemds_logging = SystemLogging(ml_system)
    systemds_logging.start_logging()
    run_systemds_lr(systemds_logging)
    systemds_logging.stop_logging()

    source = f'/tmp/local_logs/{ml_system}.log'
    destination = f'./local_logs/{ml_system}.log'

    shutil.copy(source, destination)