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
                if not hbase.Create(nombre_tabla, tabla_columns_families):  # Intenta crear la tabla
                    print(f">> La tabla <{nombre_tabla}> no existe")
                else:
                    print(f">> La tabla <{nombre_tabla}> se ha creado")

            elif comando[0] == "list":  # Si el comando es "list"
                print(f"TABLA(s)")
                for name in hbase.List():  # Lista los nombres de las tablas
                    print(f">> {str(name)}")  # Imprime cada nombre de tabla

            elif comando[0] == "disable":
                nombre_tabla = comando[1][1:-1]  # Obtiene el nombre de la tabla, eliminando las comillas
                if not hbase.Disable(nombre_tabla):  # Intenta deshabilitar la tabla
                    print(f">> La tabla <{nombre_tabla}> ya existe.")  # Imprime un mensaje si la tabla ya existe
                else:
                    print(f">> La tabla {nombre_tabla} se ha deshabilitado.")  # Imprime un mensaje si la tabla se ha deshabilitado

            elif comando[0] == "enable":
                nombre_tabla = comando[1][1:-1]  # Obtiene el nombre de la tabla, eliminando las comillas
                if not hbase.Enable(nombre_tabla):  # Intenta habilitar la tabla
                    print(f">> La tabla <{nombre_tabla}> no existe.")
                else:
                    print(f">> La tabla <{nombre_tabla}> se ha habilitado.")

            elif comando[0] == "is_enable":
                nombre_tabla = comando[1][1:-1] # Obteine el nombre de la tabla, eliminando las comillas
                print(f">> {str(hbase.Is_enable(nombre_tabla))}") # muestra el estado del elemento (true -> enable, false -> disable)

            elif comando[0] == "alter":
                nombre_tabla = comando[1][1:-2]
                contenido = " ".join(comando[2:])
                claves = [-1,-1]
                for i in range(0, len(contenido)):
                    if contenido[i] == "{":
                        claves[0] = i
                    elif contenido[i] == "}":
                        claves[1] = i
                contenido = contenido[claves[0]+1:claves[1]]
                contenido = contenido.split(" => ")
                if len(contenido) != 2:
                    print(f">> Error con el comando.")
                elif contenido[0].upper() == "NAME":
                    if hbase.Alter_table_name(nombre_tabla, contenido[1][1:-1]):
                        print(f">> Se ha modificado la tabla <{nombre_tabla}> a <{contenido[1][1:-1]}>")
                    else:
                        print(f">> Ha ocurrido un error en la modificacion, nombre repetido")
                elif contenido[0].upper() == "ADD":
                    if ":" not in contenido[1][1:-1]:
                        if hbase.Alter_table_add(nombre_tabla, contenido[1][1:-1]):
                            print(f">> Se ha agregado la familia <{contenido[1][1:-1]}> a la tabla <{nombre_tabla}>")
                        else:
                            print(f">> Ha ocurrido un error, tabla inexistente.")
                    else:
                        familia_columnas, columna = contenido[1][1:-1].split(":")
                        if hbase.Alter_table_add_column(nombre_tabla, familia_columnas, columna):
                            print(f">> Se ha agregado la columna <{columna}> a la familia <{familia_columnas}> de la tabla <{nombre_tabla}>")
                        else:
                            print(f">> Ha ocurrido un error, tabla inexistente.")
                elif contenido[0] == "DELETE":
                    if hbase.Alter_table_delete(nombre_tabla, contenido[1][1:-1]):
                        print(f">> Se ha eliminado la familia <{contenido[1][1:-1]}> a la tabla <{nombre_tabla}>")
                    else:
                        print(f">> Ha ocurrido un error, tabla inexistente")

            elif comando[0] == "describe":
                contenido = comando[1][1:-1]
                res = hbase.Describe(contenido)
                if not res:
                    print(f">> Error al realizar el describe de la tabla <{comando[1]}>, No existe")

            elif comando[0] == "help":  # Si el comando es "help"
                print(
                    "\n\n============ COMANDOS PERMITIDOS ============\n\n"+
                    "\n============== FUNCIONES DDL ==============\n"+
                    "\n\tcreate 'nombre_tabla','familia_columna1'"+
                    "\n\tlist"+
                    "\n\tdisable 'nombre_tabla' "+
                    "\n\tenable 'nombre_tabla' "+
                    "\n\tis_enable 'nombre_tabla' "+
                    "\n\talter 'nombre_tabla', {NAME => 'nuevo_nombre_tabla'}, DEBE HABER ESPACIO ENTRE LA , Y {" +
                    "\n\talter 'nombre_tabla', {ADD => 'nueva_columna'}, DEBE HABER ESPACIO ENTRE LA , Y {" +
                    "\n\talter 'nombre_tabla', {DELETE => 'columna'}, DEBE HABER ESPACIO ENTRE LA , Y {" +
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
