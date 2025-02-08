import time
import csv


def track_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start the timer
        result = func(*args, **kwargs)  # Execute the function
        end_time = time.time()  # End the timer
        elapsed_time = end_time - start_time  # Time difference

        # Convert to minutes and seconds
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60

        print(
            f"Function '{func.__name__}' took {minutes} minute(s) and {seconds:.2f} second(s)."
        )

        function_log_file = "logs/function_execution_log.csv"

        log_message = {"function": func.__name__, "time": seconds}
        with open(function_log_file, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["function", "time"])

            # Escrever o cabeçalho apenas se o arquivo estiver vazio
            if f.tell() == 0:
                writer.writeheader()

            # Escrever a linha de log
            writer.writerow(log_message)
        return result

    return wrapper
