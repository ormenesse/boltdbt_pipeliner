import logging

def process_data(self, input_tables):
    logging.info(f"{self.logging_string} - Initializing processing...")
    result_df = input_tables['flat'].select(
        *input_tables['flat'].columns
    )
    
    return result_df