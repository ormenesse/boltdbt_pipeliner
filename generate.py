import sys
import scripts.generate_airflow_dags_code as airflowgen
import scripts.generate_documentation as docgen
import scripts.generate_layers as layergen
import scripts.generate_notebook as notegen
import scripts.generate_snowflake_ddls as ddl

if __name__ == "__main__":
    scripts = sys.argv[1:] if len(sys.argv) > 1 else None
    valid_options = ['airflow', 'documentation', 'layers', 'notebook', "snowflakeddl",'all']
    
    if not scripts or not any(script in valid_options for script in scripts):
        print("You should choose one or many of the following options:\n")
        print("1. Airflow (airflow)")
        print("2. Documentation (documentation)")
        print("3. ETL layers (layers)")
        print("4. ETL Notebook (notebook)")
        print("5. Snowflake DDL (snowflakeddl)")
        print("6. ALL (all)\n")
    else:
        if 'airflow' in scripts or 'all' in scripts:
            airflowgen.create_layer_scripts("configs/etl_config.yaml")
        if 'documentation' in scripts or 'all' in scripts:
            docgen.gen_doc()
        if 'layers' in scripts or 'all' in scripts:
            layergen.create_layer_scripts("configs/etl_config.yaml")
        if 'notebook' in scripts or 'all' in scripts:
            notegen.create_etl_notebook("configs/etl_config.yaml")
        if 'snowflakeddl' in scripts or 'all' in scripts:
            ddl.create_ddls_from_schema()
        print("\nGood Dag!")
        print("""
⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⣀⡀⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣴⠒⡩⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢉⡲⣄⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢀⠼⢇⣶⠁⠀⠀⠀⠠⠀⠀⠀⠀⢀⣄⠀⠀⠳⣌⠑⢄⠀⠀⠀⠀
⠀⣀⡠⠔⢃⢴⣇⠃⢀⣾⣿⣿⠔⠀⠀⠀⠀⢾⣿⣿⡦⠀⣟⣧⡌⠹⣄⡀⠀
⢠⠏⢀⠀⠁⢸⡼⠀⠀⠉⢉⠋⠀⠀⠀⠀⠀⠈⢂⠉⠀⠀⠹⣿⠠⡐⢌⢻⠤ 
⣺⡀⠐⠡⠃⢸⡇⠀⡀⠄⠁⠀⣴⣿⣿⣿⣷⡄⠀⠃⢀⡀⠀⣿⡆⡨⡑⢽⠃
⠸⣗⡀⠈⡔⢹⡇⠈⣤⠀⠀⠀⠸⢿⣿⣿⠿⠁⠀⠀⣀⡘⢸⣿⠁⢐⢔⢼⡇
⠀⠻⣤⠚⢁⣸⡗⡄⠻⣧⡀⠀⠀⣀⣾⣕⡀⠀⠀⣴⣿⠃⣼⡿⡠⣓⡴⠛⠁
⠀⠀⠙⣭⣝⣿⣧⠆⠘⣹⡌⢽⠉⠁⠘⠀⠉⣿⠋⢰⠃⠀⣿⡇⢅⠏⠃⠀⠀
⠀⠀⠀⠀⣷⡇⢹⢢⠀⠸⡳⡘⡆⠀⠀⠀⢰⠏⣠⠏⢰⡷⠀⢛⠎⠀⠀⠀⠀
⠀⠀⠀⠀⣾⡀⠈⣞⣄⠀⠑⡑⣝⢤⣀⠠⣪⣾⠋⣀⡌⠃⢀⣮⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢸⣧⠀⠹⡟⡇⡀⠈⠊⢷⣶⡾⠏⠈⢈⣿⠇⠀⡘⡞⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⢫⡃⠀⠘⠋⡧⠄⣄⢦⡀⣄⢀⡴⠰⠏⠀⠰⣱⠃⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠑⠄⠀⠈⠁⠀⠉⢸⠏⠻⢂⠔⠁⠀⠀⠕⠁⠀⠀⠀⠀⠀⠀
""")
        print("\nGeneration completed!\n")
        