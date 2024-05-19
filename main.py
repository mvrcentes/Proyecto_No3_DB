from HBase import HBase

def main():
    """
    Función principal que actúa como simulador de comandos para HBase.

    Crea una instancia de HBase y permite la entrada de comandos en un bucle continuo.
    Los comandos admitidos permiten la creación de tablas, listado de tablas, y otras operaciones DDL y DML.
    """
    hbase = HBase()  # Crea una instancia de HBase
    while True:  # Bucle continuo para recibir comandos
        try:
            comando = input(">>")  # Espera por la entrada del comando
            if comando == "exit":  # Si el comando es "exit", termina el simulador
                print("Fin del simulador")
                break
            comando  = comando.split(" ")  # Divide el comando en palabras
            while "" in comando:  # Remueve entradas vacías del comando
                comando.remove("")
            if comando[0] == "create":  # Si el comando es "create"
                contenido = "".join(comando[1:])  # Junta los elementos del comando
                contenido = contenido.split(",")  # Divide por comas para obtener nombre y familias de columnas
                nombre_tabla = contenido[0][1:-1]  # Obtiene el nombre de la tabla
                tabla_columns_families = contenido[1:]  # Obtiene las familias de columnas
                for i in range(0, len(tabla_columns_families)):
                    tabla_columns_families[i] = tabla_columns_families[i][1:-1]  # Limpia las cadenas de texto
                if not hbase.create(nombre_tabla, tabla_columns_families):  # Intenta crear la tabla
                    print(f">> La tabla <{nombre_tabla}> no existe")
                else:
                    print(f">> La tabla <{nombre_tabla}> se ha creado")

            elif comando[0] == "list":  # Si el comando es "list"
                for name in hbase.list():  # Lista los nombres de las tablas
                    print(f">> {str(name)}")

            elif comando[0] == "help":  # Si el comando es "help"
                print(
                    "\n\n============ COMANDOS PERMITIDOS ============\n\n"+
                    "\n============== FUNCIONES DDL ==============\n"+
                    "\n\tcreate 'nombre_tabla','familia_columna1','familia_columna2',..."+
                    "\n\tlist"+
                    "\n\tdisable"+
                    "\n\tis_enable"+
                    "\n\talter"+
                    "\n\tdrop"+
                    "\n\tdrop_all"+
                    "\n\tdescribe"+
                    "\n\n ============= FUNCIONES DML =============\n"+
                    "\n\tput"+
                    "\n\tget"+
                    "\n\tscan"+
                    "\n\tdelete"+
                    "\n\tdelete_all"+
                    "\n\tcount"+
                    "\n\ttruncate"
                )
        except Exception as e:  # Manejo de errores
            print(">> Comando no reconocido ", e)

if __name__ == "__main__":
    main()  # Ejecuta la función principal
