using System;
using System.Data.OleDb;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace AgenteNominaManual
{
    class Program
    {
        // Rutas dinámicas y Credenciales (Ya no están fijas en el código)
        static string RutaBase = "";
        static string RutaHonorarios = "";
        static string BaseEndpointUrl = "";
        static string BackupEndpointUrl = "";
        static string ApiKeySecreta = ""; // La llave de seguridad

        static readonly string ColumnaPeriodo = "PERIODO";

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== AGENTE DE SINCRONIZACIÓN INJUVE (NÓMINA Y CATÁLOGOS) ===\n");

            if (!CargarConfiguracion())
            {
                Console.WriteLine("\nPresiona cualquier tecla para salir...");
                Console.ReadKey();
                return;
            }

            try
            {
                Console.WriteLine("\n==================================================");
                Console.WriteLine("          ZONA 1: BASE Y CONFIANZA");
                Console.WriteLine("==================================================");

                // 1. Catálogo de Base
                await ProcesarCatalogo("CATÁLOGO DE EMPLEADOS (BASE)", RutaBase, "mnom01");

                // 2. Nómina de Base
                await ProcesarNomina("NÓMINA (BASE Y CONFIANZA)", RutaBase, "mnom12");

                // 3. Respaldo MDB Base
                Console.WriteLine("\n--------------------------------------------------");
                Console.Write("¿Deseas realizar un respaldo del archivo .mdb de BASE Y CONFIANZA en el servidor? (S/N): ");
                string respuestaRespaldo = Console.ReadLine()?.Trim().ToUpper();

                if (respuestaRespaldo == "S")
                {
                    await RespaldarMdbAsync(RutaBase);
                }

                Console.WriteLine("\n==================================================");
                Console.WriteLine("              ZONA 2: HONORARIOS");
                Console.WriteLine("==================================================");

                // 4. Catálogo de Honorarios
                await ProcesarCatalogo("CATÁLOGO DE EMPLEADOS (HONORARIOS)", RutaHonorarios, "mnom01h");

                // 5. Nómina de Honorarios
                await ProcesarNomina("NÓMINA (HONORARIOS)", RutaHonorarios, "mnom12h");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\nERROR CRÍTICO: {ex.Message}");
                Console.ResetColor();
            }

            Console.WriteLine("\n=== PROCESO TERMINADO ===");
            Console.WriteLine("Presiona cualquier tecla para salir...");
            Console.ReadKey();
        }

        // ==========================================
        // MÉTODOS PARA EL CATÁLOGO DE EMPLEADOS
        // ==========================================

        static async Task ProcesarCatalogo(string nombreCatalogo, string rutaAccess, string coleccionMongo)
        {
            Console.WriteLine($"\n--- Revisando: {nombreCatalogo} ---");
            string connectionString = $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={rutaAccess};";

            Console.Write($"¿Deseas extraer y enviar el {nombreCatalogo} al portal web? (S/N): ");
            string respuesta = Console.ReadLine()?.Trim().ToUpper();

            if (respuesta == "S")
            {
                string tempCsvPath = Path.Combine(Path.GetTempPath(), $"{coleccionMongo}_{DateTime.Now:HHmmss}.csv");

                Console.WriteLine("Extrayendo todos los empleados y generando CSV...");
                GenerarCsvCatalogo(tempCsvPath, connectionString);

                Console.WriteLine("Enviando catálogo al portal...");
                await EnviarAlPortalAsync(tempCsvPath, coleccionMongo);

                if (File.Exists(tempCsvPath)) File.Delete(tempCsvPath);

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"¡El {nombreCatalogo} se procesó y envió con éxito!");
                Console.ResetColor();
            }
            else
            {
                Console.WriteLine($"Operación omitida para el {nombreCatalogo}.");
            }
        }

        static void GenerarCsvCatalogo(string rutaDestino, string connectionString)
        {
            string query = "SELECT * FROM mnom01";

            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                OleDbCommand command = new OleDbCommand(query, connection);
                connection.Open();

                using (OleDbDataReader reader = command.ExecuteReader())
                using (StreamWriter writer = new StreamWriter(rutaDestino, false, Encoding.UTF8))
                {
                    int numColumnas = reader.FieldCount;
                    string[] nombresColumnas = new string[numColumnas];
                    for (int i = 0; i < numColumnas; i++)
                    {
                        nombresColumnas[i] = reader.GetName(i).Replace("[", "").Replace("]", "");
                    }
                    writer.WriteLine(string.Join(",", nombresColumnas));

                    while (reader.Read())
                    {
                        string[] fila = new string[numColumnas];
                        for (int i = 0; i < numColumnas; i++)
                        {
                            if (reader.IsDBNull(i))
                            {
                                fila[i] = "";
                            }
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                            {
                                fila[i] = reader.GetDateTime(i).ToString("dd/MM/yyyy");
                            }
                            else
                            {
                                string valor = reader[i].ToString();
                                valor = valor.Replace(",", " ").Replace("\r", "").Replace("\n", " ");
                                fila[i] = valor;
                            }
                        }
                        writer.WriteLine(string.Join(",", fila));
                    }
                }
            }
        }

        // ==========================================
        // MÉTODOS PARA LA NÓMINA
        // ==========================================

        static async Task ProcesarNomina(string nombreNomina, string rutaAccess, string coleccionMongo)
        {
            Console.WriteLine($"\n--- Revisando: {nombreNomina} ---");
            string connectionString = $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={rutaAccess};";

            string ultimoPeriodo = ObtenerUltimoPeriodo(connectionString);

            if (string.IsNullOrEmpty(ultimoPeriodo))
            {
                Console.WriteLine($"No se encontraron periodos en la base de datos de {nombreNomina}.");
                return;
            }

            Console.WriteLine($"Se detectó que el último periodo es: [{ultimoPeriodo}]");
            Console.Write($"¿Deseas extraer y enviar la {nombreNomina} al portal web? (S/N): ");
            string respuesta = Console.ReadLine()?.Trim().ToUpper();

            if (respuesta == "S")
            {
                string tempCsvPath = Path.Combine(Path.GetTempPath(), $"{coleccionMongo}_{ultimoPeriodo}_{DateTime.Now:HHmmss}.csv");

                Console.WriteLine("Extrayendo datos y generando CSV...");
                GenerarCsv(tempCsvPath, ultimoPeriodo, connectionString, coleccionMongo);

                Console.WriteLine("Enviando archivo al portal...");
                await EnviarAlPortalAsync(tempCsvPath, coleccionMongo);

                if (File.Exists(tempCsvPath)) File.Delete(tempCsvPath);

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"¡La {nombreNomina} se procesó y envió con éxito!");
                Console.ResetColor();
            }
            else
            {
                Console.WriteLine($"Operación omitida para la {nombreNomina}.");
            }
        }

        static string ObtenerUltimoPeriodo(string connectionString)
        {
            string query = $"SELECT MAX({ColumnaPeriodo}) FROM mnom12";
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                OleDbCommand command = new OleDbCommand(query, connection);
                connection.Open();
                object resultado = command.ExecuteScalar();
                return resultado?.ToString() ?? "";
            }
        }

        static void GenerarCsv(string rutaDestino, string periodo, string connectionString, string coleccion)
        {
            string query = "";

            if (coleccion == "mnom12")
            {
                string columnasBase = "EMPLEADO, DEPTO, CAT, PUESTO, PROGRAMA, SUBPROGRAMA, META, ACCION, PERCDESC, IMPORTE, DESCRIPCION, NUMDESC, CHEQUE, DIASTRA, EXENTO, TIPOEMP, SUECOM, MPIO, PERIODO, FECHDES, FECHHAS, FECHAP, [AÑO], PERPAGO, DIASHAB, DIASADI, SUELDODIA, ISPT, SUBSIDIO, CREDITO, ISSSTEPAT, SERMEDPAT, FONPENPAT, ACCTRAPAT, CONADIPAT, ACTINST, CLAVEPRESUP, HSDOBLES, HSTRIPLES, IMPHSDOBLES, IMPHSTRIPLES, IMPHSEXTRASGRAV, TIPONOM, YASETIMBRO, ISPTSUELDO, ISPTOTRASPERC, BANCO, MESPAGADO, RECIBO, CUOTAPAT, RFC, PROISSSTECALI";
                query = $"SELECT {columnasBase} FROM mnom12 WHERE {ColumnaPeriodo} = {periodo}";
            }
            else
            {
                query = $"SELECT * FROM mnom12 WHERE {ColumnaPeriodo} = {periodo}";
            }

            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                OleDbCommand command = new OleDbCommand(query, connection);
                connection.Open();

                using (OleDbDataReader reader = command.ExecuteReader())
                using (StreamWriter writer = new StreamWriter(rutaDestino, false, Encoding.UTF8))
                {
                    int numColumnas = reader.FieldCount;
                    string[] nombresColumnas = new string[numColumnas];
                    for (int i = 0; i < numColumnas; i++)
                    {
                        nombresColumnas[i] = reader.GetName(i).Replace("[", "").Replace("]", "");
                    }
                    writer.WriteLine(string.Join(",", nombresColumnas));

                    while (reader.Read())
                    {
                        string[] fila = new string[numColumnas];
                        for (int i = 0; i < numColumnas; i++)
                        {
                            if (reader.IsDBNull(i))
                            {
                                fila[i] = "";
                            }
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                            {
                                fila[i] = reader.GetDateTime(i).ToString("dd/MM/yyyy");
                            }
                            else
                            {
                                string valor = reader[i].ToString();
                                valor = valor.Replace(",", " ").Replace("\r", "").Replace("\n", " ");
                                fila[i] = valor;
                            }
                        }
                        writer.WriteLine(string.Join(",", fila));
                    }
                }
            }
        }

        // ==========================================
        // MÉTODOS DE RED Y UTILIDAD (CON SEGURIDAD AÑADIDA)
        // ==========================================

        static bool CargarConfiguracion()
        {
            string rutaApp = AppDomain.CurrentDomain.BaseDirectory;
            string rutaConfig = Path.Combine(rutaApp, "config.txt");

            if (File.Exists(rutaConfig))
            {
                string[] lineas = File.ReadAllLines(rutaConfig);
                if (lineas.Length >= 5) // Ahora esperamos 5 datos de configuración
                {
                    RutaBase = lineas[0].Trim();
                    RutaHonorarios = lineas[1].Trim();
                    BaseEndpointUrl = lineas[2].Trim();
                    BackupEndpointUrl = lineas[3].Trim();
                    ApiKeySecreta = lineas[4].Trim();
                    return true;
                }
            }

            // Si no existe o está incompleto, pedimos todos los datos
            Console.WriteLine("=== CONFIGURACIÓN INICIAL (SEGURIDAD) ===");
            Console.WriteLine("No se encontró el archivo config.txt o está incompleto.");

            Console.WriteLine("\n1. Ruta COMPLETA de la base de datos de BASE/CONFIANZA:");
            Console.Write("> ");
            RutaBase = Console.ReadLine()?.Trim();

            Console.WriteLine("\n2. Ruta COMPLETA de la base de datos de HONORARIOS:");
            Console.Write("> ");
            RutaHonorarios = Console.ReadLine()?.Trim();

            Console.WriteLine("\n3. URL base del Endpoint para CSV (Ej: https://juventudbc.com.mx/api/backend/upload/):");
            Console.Write("> ");
            BaseEndpointUrl = Console.ReadLine()?.Trim();

            Console.WriteLine("\n4. URL del Endpoint para Respaldo MDB (Ej: https://juventudbc.com.mx/api/backend/upload/backup-mdb):");
            Console.Write("> ");
            BackupEndpointUrl = Console.ReadLine()?.Trim();

            Console.WriteLine("\n5. API KEY Secreta (La contraseña para conectar con tu servidor Node.js):");
            Console.Write("> ");
            ApiKeySecreta = Console.ReadLine()?.Trim();

            if (string.IsNullOrEmpty(RutaBase) || string.IsNullOrEmpty(BaseEndpointUrl) || string.IsNullOrEmpty(ApiKeySecreta))
            {
                Console.WriteLine("\nError: Faltan datos obligatorios.");
                return false;
            }

            File.WriteAllLines(rutaConfig, new string[] { RutaBase, RutaHonorarios, BaseEndpointUrl, BackupEndpointUrl, ApiKeySecreta });
            Console.WriteLine("\n¡Configuración y credenciales guardadas de forma local en config.txt!\n");
            return true;
        }

        static async Task EnviarAlPortalAsync(string rutaArchivo, string coleccion)
        {
            string urlFinal = BaseEndpointUrl + coleccion;

            using (HttpClient client = new HttpClient())
            {
                // Agregamos la llave secreta en las cabeceras de la petición
                client.DefaultRequestHeaders.Add("x-api-key", ApiKeySecreta);

                using (var multipartFormContent = new MultipartFormDataContent())
                {
                    var fileStreamContent = new StreamContent(File.OpenRead(rutaArchivo));
                    fileStreamContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("text/csv");

                    multipartFormContent.Add(fileStreamContent, name: "archivo", fileName: Path.GetFileName(rutaArchivo));

                    HttpResponseMessage response = await client.PostAsync(urlFinal, multipartFormContent);

                    if (!response.IsSuccessStatusCode)
                    {
                        string error = await response.Content.ReadAsStringAsync();
                        throw new Exception($"El servidor rechazó el archivo. Código: {response.StatusCode}. Detalle: {error}");
                    }
                }
            }
        }

        static async Task RespaldarMdbAsync(string rutaMdb)
        {
            Console.WriteLine($"\n--- Iniciando respaldo de la base de datos de BASE Y CONFIANZA ---");
            Console.WriteLine($"Archivo a respaldar: {rutaMdb}");

            try
            {
                using (HttpClient client = new HttpClient())
                {
                    client.Timeout = TimeSpan.FromMinutes(10);

                    // Agregamos la llave secreta en las cabeceras de la petición
                    client.DefaultRequestHeaders.Add("x-api-key", ApiKeySecreta);

                    using (var multipartFormContent = new MultipartFormDataContent())
                    {
                        var fileStream = new FileStream(rutaMdb, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                        var fileStreamContent = new StreamContent(fileStream);

                        fileStreamContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/x-msaccess");

                        multipartFormContent.Add(fileStreamContent, name: "archivo", fileName: Path.GetFileName(rutaMdb));

                        HttpResponseMessage response = await client.PostAsync(BackupEndpointUrl, multipartFormContent);

                        if (response.IsSuccessStatusCode)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine("¡Respaldo de base de datos enviado y guardado con éxito!");
                            Console.ResetColor();
                        }
                        else
                        {
                            string error = await response.Content.ReadAsStringAsync();
                            throw new Exception($"El servidor rechazó el respaldo. Código: {response.StatusCode}. Detalle: {error}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error al intentar respaldar el archivo .mdb: {ex.Message}");
                Console.ResetColor();
            }
        }
    }
}