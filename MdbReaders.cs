using System.Diagnostics;
using System.Globalization;
using System.Runtime.Versioning;

namespace AgenteNominaManual
{
    // Abstracción sobre la lectura de archivos .mdb.
    // Cada celda devuelta ya viene formateada como string:
    //   - null               -> ""
    //   - DateTime           -> "dd/MM/yyyy"
    //   - resto              -> ToString() del valor crudo
    // La sanitización CSV (quitar comas / CR / LF) se hace en el writer, no aquí.
    interface IMdbReader
    {
        (string[] Headers, IEnumerable<string[]> Rows) ReadTable(
            string mdbPath,
            string tableName,
            string[]? columns = null,
            (string Column, string Value)? filter = null);
    }

#if WINDOWS_OLEDB
    [SupportedOSPlatform("windows")]
    sealed class WindowsOleDbReader : IMdbReader
    {
        public (string[] Headers, IEnumerable<string[]> Rows) ReadTable(
            string mdbPath,
            string tableName,
            string[]? columns = null,
            (string Column, string Value)? filter = null)
        {
            string colList = (columns is { Length: > 0 }) ? string.Join(", ", columns) : "*";
            string query = $"SELECT {colList} FROM {tableName}";
            if (filter is { } f)
                query += $" WHERE {f.Column} = {f.Value}";

            string connectionString = $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={mdbPath};";

            var connection = new System.Data.OleDb.OleDbConnection(connectionString);
            var command = new System.Data.OleDb.OleDbCommand(query, connection);
            connection.Open();
            var reader = command.ExecuteReader();

            int n = reader.FieldCount;
            string[] headers = new string[n];
            for (int i = 0; i < n; i++)
                headers[i] = reader.GetName(i).Replace("[", "").Replace("]", "");

            IEnumerable<string[]> Iterate()
            {
                try
                {
                    while (reader.Read())
                    {
                        var row = new string[n];
                        for (int i = 0; i < n; i++)
                        {
                            if (reader.IsDBNull(i))
                                row[i] = "";
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                                row[i] = reader.GetDateTime(i).ToString("dd/MM/yyyy");
                            else
                                row[i] = reader[i].ToString() ?? "";
                        }
                        yield return row;
                    }
                }
                finally
                {
                    reader.Dispose();
                    command.Dispose();
                    connection.Dispose();
                }
            }

            return (headers, Iterate());
        }
    }
#endif

    // Implementación para macOS / Linux. Invoca el binario `mdb-export` (paquete
    // mdbtools) por Process y parsea su salida en TSV. No soporta WHERE en el
    // servidor: si se pasa filter, se aplica en C# tras leer la fila.
    sealed class MdbToolsReader : IMdbReader
    {
        public static bool IsAvailable()
        {
            try
            {
                using var p = Process.Start(new ProcessStartInfo
                {
                    FileName = "mdb-export",
                    Arguments = "--help",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                });
                if (p == null) return false;
                p.WaitForExit(2000);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public (string[] Headers, IEnumerable<string[]> Rows) ReadTable(
            string mdbPath,
            string tableName,
            string[]? columns = null,
            (string Column, string Value)? filter = null)
        {
            var psi = new ProcessStartInfo
            {
                FileName = "mdb-export",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            psi.ArgumentList.Add("-Q");                  // sin comillas en texto
            psi.ArgumentList.Add("-d"); psi.ArgumentList.Add("\t");        // delimitador tab
            psi.ArgumentList.Add("-D"); psi.ArgumentList.Add("%d/%m/%Y");  // formato de fecha
            psi.ArgumentList.Add("-b"); psi.ArgumentList.Add("strip");     // ignorar binarios
            psi.ArgumentList.Add(mdbPath);
            psi.ArgumentList.Add(tableName);

            var proc = Process.Start(psi)
                ?? throw new InvalidOperationException(
                    "No se pudo iniciar mdb-export. Asegúrate de tener mdbtools instalado (brew install mdbtools).");

            string? headerLine = proc.StandardOutput.ReadLine();
            if (headerLine == null)
            {
                proc.WaitForExit();
                string err = proc.StandardError.ReadToEnd();
                throw new InvalidOperationException(
                    $"mdb-export no devolvió datos para la tabla '{tableName}'. stderr: {err}");
            }

            string[] rawHeaders = headerLine.Split('\t');

            int filterIdx = -1;
            if (filter is { } f)
            {
                for (int i = 0; i < rawHeaders.Length; i++)
                {
                    if (string.Equals(rawHeaders[i], f.Column, StringComparison.OrdinalIgnoreCase))
                    {
                        filterIdx = i;
                        break;
                    }
                }
                if (filterIdx < 0)
                    throw new InvalidOperationException(
                        $"La columna de filtro '{f.Column}' no existe en '{tableName}'.");
            }

            int[] projection;
            string[] outHeaders;
            if (columns is { Length: > 0 })
            {
                projection = new int[columns.Length];
                outHeaders = new string[columns.Length];
                for (int i = 0; i < columns.Length; i++)
                {
                    int idx = Array.FindIndex(rawHeaders,
                        h => string.Equals(h, columns[i], StringComparison.OrdinalIgnoreCase));
                    if (idx < 0)
                        throw new InvalidOperationException(
                            $"La columna '{columns[i]}' no existe en '{tableName}'.");
                    projection[i] = idx;
                    outHeaders[i] = rawHeaders[idx];
                }
            }
            else
            {
                projection = Enumerable.Range(0, rawHeaders.Length).ToArray();
                outHeaders = rawHeaders;
            }

            string? filterValue = filter?.Value;

            IEnumerable<string[]> Iterate()
            {
                try
                {
                    string? line;
                    while ((line = proc.StandardOutput.ReadLine()) != null)
                    {
                        string[] raw = line.Split('\t');
                        if (filterIdx >= 0 && filterIdx < raw.Length)
                        {
                            if (!string.Equals(raw[filterIdx].Trim(), filterValue?.Trim(),
                                    StringComparison.OrdinalIgnoreCase))
                                continue;
                        }
                        var row = new string[projection.Length];
                        for (int i = 0; i < projection.Length; i++)
                        {
                            int src = projection[i];
                            row[i] = src < raw.Length ? raw[src] : "";
                        }
                        yield return row;
                    }
                    proc.WaitForExit();
                    if (proc.ExitCode != 0)
                    {
                        string err = proc.StandardError.ReadToEnd();
                        throw new InvalidOperationException(
                            $"mdb-export terminó con código {proc.ExitCode}. stderr: {err}");
                    }
                }
                finally
                {
                    if (!proc.HasExited)
                    {
                        try { proc.Kill(); } catch { }
                    }
                    proc.Dispose();
                }
            }

            return (outHeaders, Iterate());
        }
    }
}
