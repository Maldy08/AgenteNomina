using System.Globalization;
using System.Text;

namespace AgenteNominaManual
{
    class Program
    {
        // Rutas dinámicas y Credenciales
        static string RutaBase = "";
        static string RutaHonorarios = "";
        static string BaseEndpointUrl = "";
        static string BackupEndpointUrl = "";
        static string ApiKeySecreta = "";

        static readonly string ColumnaPeriodo = "PERIODO";

        // Lector de .mdb seleccionado según el SO. En Windows usa OleDb + Jet
        // (32-bit); en macOS/Linux invoca mdb-export (paquete mdbtools).
        static readonly IMdbReader Reader = CreateReader();

        static IMdbReader CreateReader()
        {
#if WINDOWS_OLEDB
            if (OperatingSystem.IsWindows())
                return new WindowsOleDbReader();
#endif
            return new MdbToolsReader();
        }

        // ==========================================
        // CATÁLOGOS COMPARTIDOS (ZONA 3)
        // ==========================================
        // Tabla de mapeo de catálogos auxiliares que también deben sincronizarse al
        // backend. Estos NO son específicos de base ni honorarios sino catálogos
        // generales (departamentos, niveles tabulares, etc).
        //
        // Cada entrada define:
        //   - Descripcion:       texto legible para los prompts.
        //   - MongoCollection:   colección destino en Mongo (segmento de la URL del backend).
        //   - AccessTable:       nombre de la tabla en el archivo .mdb.
        //   - MdbSource:         "base" o "honorarios" — de qué .mdb leerla.
        //
        // ⚠️ Asunciones por defecto (ajustar si alguna es incorrecta):
        //   1. Las 5 viven en el MDB BASE.
        //   2. El nombre de la tabla en Access es idéntico al de la colección en Mongo.
        // Si el legacy llama distinto a alguna tabla, basta cambiar AccessTable; si
        // alguna está en el MDB de honorarios, cambiar MdbSource a "honorarios".
        //
        // Se omiten del array (NO existen en Access, se mantienen por otra vía):
        //   - "bss": se carga vía Excel (POST /upload/bss-excel) en el backend.
        //   - "sueldoprestacionesbase" / "sueldoprestacionesconf": se administran vía CRUD REST
        //     (POST/PUT/DELETE por registro) desde el frontend. Intentar leerlas desde el .mdb
        //     truena porque la tabla no existe — son datos capturados manualmente, no espejados
        //     del legacy. Si en el futuro se decide poblarlas desde Access, agregar la línea aquí.
        record CatalogoAdicional(string Descripcion, string MongoCollection, string AccessTable, string MdbSource);

        static readonly CatalogoAdicional[] CatalogosAdicionales = new[]
        {
            new CatalogoAdicional("CATEGORÍAS / PUESTOS (mnom03)",      "mnom03",                 "mnom03",                 "base"),
            new CatalogoAdicional("DEPARTAMENTOS (mnom04)",             "mnom04",                 "mnom04",                 "base"),
            new CatalogoAdicional("PUESTOS EXTENDIDOS (mnom90)",        "mnom90",                 "mnom90",                 "base"),
            new CatalogoAdicional("NIVELES TABULARES (BASE)",           "niveles",                "niveles",                "base"),
            new CatalogoAdicional("NIVELES TABULARES (CONFIANZA)",      "nivelesconfianza",       "nivelesconfianza",       "base"),
        };

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== AGENTE DE SINCRONIZACIÓN INJUVE (NÓMINA Y CATÁLOGOS) ===\n");

            if (!OperatingSystem.IsWindows() && !MdbToolsReader.IsAvailable())
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: Este agente requiere 'mdbtools' para leer archivos .mdb en este sistema.");
                Console.WriteLine("Instala con:  brew install mdbtools   (macOS)");
                Console.WriteLine("              sudo apt install mdbtools   (Debian/Ubuntu)");
                Console.ResetColor();
                Console.WriteLine("\nPresiona cualquier tecla para salir...");
                Console.ReadKey();
                return;
            }

            if (!CargarConfiguracion())
            {
                Console.WriteLine("\nPresiona cualquier tecla para salir...");
                Console.ReadKey();
                return;
            }

            Console.WriteLine("\n==================================================");
            Console.WriteLine("          ZONA 1: BASE Y CONFIANZA");
            Console.WriteLine("==================================================");

            // 1. Catálogo de Base
            await EjecutarPasoAsync(() => ProcesarCatalogo("CATÁLOGO DE EMPLEADOS (BASE)", RutaBase, "mnom01", "mnom01"));

            // 2. Nómina de Base
            await EjecutarPasoAsync(() => ProcesarNomina("NÓMINA (BASE Y CONFIANZA)", RutaBase, "mnom12"));

            // 3. Respaldo MDB Base
            Console.WriteLine("\n--------------------------------------------------");
            Console.Write("¿Deseas realizar un respaldo del archivo .mdb de BASE Y CONFIANZA en el servidor? (S/N): ");
            string respuestaRespaldo = Console.ReadLine()?.Trim().ToUpper();

            if (respuestaRespaldo == "S")
            {
                await EjecutarPasoAsync(() => RespaldarMdbAsync(RutaBase));
            }

            Console.WriteLine("\n==================================================");
            Console.WriteLine("              ZONA 2: HONORARIOS");
            Console.WriteLine("==================================================");

            // 4. Catálogo de Honorarios
            await EjecutarPasoAsync(() => ProcesarCatalogo("CATÁLOGO DE EMPLEADOS (HONORARIOS)", RutaHonorarios, "mnom01h", "mnom01"));

            // 5. Nómina de Honorarios
            await EjecutarPasoAsync(() => ProcesarNomina("NÓMINA (HONORARIOS)", RutaHonorarios, "mnom12h"));

            Console.WriteLine("\n==================================================");
            Console.WriteLine("          ZONA 3: CATÁLOGOS COMPARTIDOS");
            Console.WriteLine("==================================================");

            Console.Write("\n¿Deseas revisar los catálogos compartidos (categorías, departamentos, puestos, niveles)? (S/N): ");
            string respuestaCatalogos = Console.ReadLine()?.Trim().ToUpper();

            if (respuestaCatalogos == "S")
            {
                foreach (var catalogo in CatalogosAdicionales)
                {
                    await EjecutarPasoAsync(() => ProcesarCatalogoAdicional(catalogo));
                }
            }
            else
            {
                Console.WriteLine("Catálogos compartidos omitidos.");
            }

            Console.WriteLine("\n=== PROCESO TERMINADO ===");
            Console.WriteLine("Presiona cualquier tecla para salir...");
            Console.ReadKey();
        }

        // Ejecuta un paso aislado: si truena, muestra el error y permite que el flujo continúe
        // con los siguientes pasos (catálogos / nóminas / respaldo) en lugar de abortar todo.
        static async Task EjecutarPasoAsync(Func<Task> paso)
        {
            try
            {
                await paso();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\nERROR en este paso: {ex.Message}");
                Console.WriteLine("Continuando con el siguiente paso...");
                Console.ResetColor();
            }
        }

        // ==========================================
        // MÉTODOS PARA EL CATÁLOGO DE EMPLEADOS
        // ==========================================

        static async Task ProcesarCatalogo(string nombreCatalogo, string rutaAccess, string coleccionMongo, string tablaAccess)
        {
            Console.WriteLine($"\n--- Revisando: {nombreCatalogo} ---");

            Console.Write($"¿Deseas extraer y enviar el {nombreCatalogo} al portal web? (S/N): ");
            string respuesta = Console.ReadLine()?.Trim().ToUpper();

            if (respuesta == "S")
            {
                string tempCsvPath = Path.Combine(Path.GetTempPath(), $"{coleccionMongo}_{DateTime.Now:HHmmss}.csv");

                Console.WriteLine($"Extrayendo registros de la tabla '{tablaAccess}' y generando CSV...");
                GenerarCsvCatalogo(tempCsvPath, rutaAccess, tablaAccess);

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

        static async Task ProcesarCatalogoAdicional(CatalogoAdicional catalogo)
        {
            string rutaMdb = catalogo.MdbSource == "honorarios" ? RutaHonorarios : RutaBase;

            if (string.IsNullOrEmpty(rutaMdb))
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"Saltando {catalogo.Descripcion}: ruta del MDB '{catalogo.MdbSource}' no configurada.");
                Console.ResetColor();
                return;
            }

            await ProcesarCatalogo(catalogo.Descripcion, rutaMdb, catalogo.MongoCollection, catalogo.AccessTable);
        }

        static void GenerarCsvCatalogo(string rutaDestino, string rutaMdb, string tablaAccess)
        {
            var (headers, rows) = Reader.ReadTable(rutaMdb, tablaAccess);
            WriteCsv(rutaDestino, headers, rows);
        }

        // ==========================================
        // MÉTODOS PARA LA NÓMINA Y PERIODOS
        // ==========================================

        static (string Periodo, string Rango) ObtenerPeriodoActivo(string rutaMdb)
        {
            try
            {
                var (_, rows) = Reader.ReadTable(rutaMdb, "PERCERRADOS",
                    columns: new[] { "PERIODO", "FECHADESDE", "FECHAHASTA" });

                foreach (var row in rows)
                {
                    // Ignoramos los periodos 100+ porque son anuales o especiales
                    if (!int.TryParse(row[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out int per) || per >= 100)
                        continue;
                    if (!DateTime.TryParseExact(row[1], "dd/MM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var inicio))
                        continue;
                    if (!DateTime.TryParseExact(row[2], "dd/MM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var fin))
                        continue;

                    // Si la fecha de hoy está dentro de este rango, ese es el periodo activo
                    if (DateTime.Now.Date >= inicio.Date && DateTime.Now.Date <= fin.Date)
                        return (row[0], $"del {inicio:dd/MM/yyyy} al {fin:dd/MM/yyyy}");
                }
            }
            catch
            {
                // Si la tabla no existe o hay un error, lo manejamos de forma silenciosa
            }

            return ("", "");
        }

        static async Task ProcesarNomina(string nombreNomina, string rutaAccess, string coleccionMongo)
        {
            Console.WriteLine($"\n--- Revisando: {nombreNomina} ---");

            // 1. Usamos la tabla PERCERRADOS para ver en qué fechas estamos hoy
            var periodoSugerido = ObtenerPeriodoActivo(rutaAccess);
            string periodoElegido = "";

            if (!string.IsNullOrEmpty(periodoSugerido.Periodo))
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"El sistema detecta que hoy estamos en el PERIODO {periodoSugerido.Periodo} ({periodoSugerido.Rango}).");
                Console.ResetColor();
                Console.Write($"> Presiona ENTER para procesar el periodo {periodoSugerido.Periodo}, o teclea otro número: ");

                string input = Console.ReadLine()?.Trim();
                // Si da Enter sin escribir nada, toma el sugerido. Si escribe algo, toma lo que escribió.
                periodoElegido = string.IsNullOrEmpty(input) ? periodoSugerido.Periodo : input;
            }
            else
            {
                // Por si falla la tabla PERCERRADOS, modo manual clásico
                Console.Write("> Escribe el número del periodo a procesar (ej. 7) y presiona Enter: ");
                periodoElegido = Console.ReadLine()?.Trim();
            }

            // Validamos que sea un número válido y que no hayan cancelado.
            // 0 (o cualquier valor <= 0) se trata como "omitir este paso".
            if (string.IsNullOrEmpty(periodoElegido) || !int.TryParse(periodoElegido, out int periodoNum) || periodoNum <= 0)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"Operación cancelada u omitida para la {nombreNomina}.");
                Console.ResetColor();
                return;
            }

            string tempCsvPath = Path.Combine(Path.GetTempPath(), $"{coleccionMongo}_{periodoElegido}_{DateTime.Now:HHmmss}.csv");

            Console.WriteLine($"Extrayendo datos únicamente del PERIODO {periodoElegido} y generando CSV...");

            // Inyectamos el periodo que eligió el usuario en el WHERE de la consulta
            GenerarCsv(tempCsvPath, periodoElegido, rutaAccess, coleccionMongo);

            Console.WriteLine("Enviando archivo al portal...");
            await EnviarAlPortalAsync(tempCsvPath, coleccionMongo);

            if (File.Exists(tempCsvPath)) File.Delete(tempCsvPath);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"¡La {nombreNomina} (Periodo {periodoElegido}) se procesó y envió con éxito!");
            Console.ResetColor();
        }

        // Columnas explícitas para la nómina de BASE/CONFIANZA: el backend las consume
        // en este orden. Para HONORARIOS (coleccion = "mnom12h") el agente envía SELECT *.
        static readonly string[] ColumnasNominaBase = new[]
        {
            "EMPLEADO", "DEPTO", "CAT", "PUESTO", "PROGRAMA", "SUBPROGRAMA", "META", "ACCION",
            "PERCDESC", "IMPORTE", "DESCRIPCION", "NUMDESC", "CHEQUE", "DIASTRA", "EXENTO", "TIPOEMP",
            "SUECOM", "MPIO", "PERIODO", "FECHDES", "FECHHAS", "FECHAP", "AÑO", "PERPAGO", "DIASHAB",
            "DIASADI", "SUELDODIA", "ISPT", "SUBSIDIO", "CREDITO", "ISSSTEPAT", "SERMEDPAT",
            "FONPENPAT", "ACCTRAPAT", "CONADIPAT", "ACTINST", "CLAVEPRESUP", "HSDOBLES", "HSTRIPLES",
            "IMPHSDOBLES", "IMPHSTRIPLES", "IMPHSEXTRASGRAV", "TIPONOM", "YASETIMBRO", "ISPTSUELDO",
            "ISPTOTRASPERC", "BANCO", "MESPAGADO", "RECIBO", "CUOTAPAT", "RFC", "PROISSSTECALI",
        };

        static void GenerarCsv(string rutaDestino, string periodo, string rutaMdb, string coleccion)
        {
            string[]? columns = (coleccion == "mnom12") ? ColumnasNominaBase : null;
            var (headers, rows) = Reader.ReadTable(rutaMdb, "mnom12", columns,
                filter: (ColumnaPeriodo, periodo));
            WriteCsv(rutaDestino, headers, rows);
        }

        // Escribe el CSV con el formato exacto que el backend ya espera:
        //   - Sin comillas.
        //   - Comas y saltos de línea dentro de un valor reemplazados por espacio.
        //   - Corchetes quitados de los nombres de columna (legado de OleDb).
        //   - Codificación UTF-8.
        static void WriteCsv(string rutaDestino, string[] headers, IEnumerable<string[]> rows)
        {
            using var writer = new StreamWriter(rutaDestino, false, Encoding.UTF8);
            writer.WriteLine(string.Join(",", headers.Select(SanitizarHeader)));
            foreach (var row in rows)
                writer.WriteLine(string.Join(",", row.Select(SanitizarValor)));
        }

        static string SanitizarHeader(string s) => s.Replace("[", "").Replace("]", "");
        static string SanitizarValor(string s) => s.Replace(",", " ").Replace("\r", "").Replace("\n", " ");

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
                if (lineas.Length >= 5)
                {
                    RutaBase = lineas[0].Trim();
                    RutaHonorarios = lineas[1].Trim();
                    BaseEndpointUrl = lineas[2].Trim();
                    BackupEndpointUrl = lineas[3].Trim();
                    ApiKeySecreta = lineas[4].Trim();
                    return true;
                }
            }

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