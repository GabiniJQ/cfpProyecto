import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    private static final String PRODUCTS_FILE = "productos.csv";
    private static final String VENDORS_FILE = "vendedores.csv";
    private static final String REPORT_VENDORS = "reporte_vendedores.csv";
    private static final String REPORT_PRODUCTS = "reporte_productos.csv";
    private static final String ERRORS_LOG = "errores_log.txt";
    private static final Charset ENCODING = Charset.defaultCharset();

    public static void main(String[] args) {
        System.out.println("=== Inicio del procesamiento de ventas ===");

        try {
            Path cwd = Paths.get(System.getProperty("user.dir"));

            Map<String, Product> products = loadProducts(cwd.resolve(PRODUCTS_FILE));
            Map<String, Vendor> vendors = loadVendors(cwd.resolve(VENDORS_FILE));
            Map<String, Double> vendorTotals = new HashMap<>();
            Map<String, Integer> productQuantities = new HashMap<>();
            List<String> errors = new ArrayList<>();

            for (String pid : products.keySet()) {
                productQuantities.put(pid, 0);
            }

            try (DirectoryStream<Path> ds = Files.newDirectoryStream(cwd, "*.csv")) {
                for (Path file : ds) {
                    String fname = file.getFileName().toString();
                    if (fname.equalsIgnoreCase(PRODUCTS_FILE) ||
                            fname.equalsIgnoreCase(VENDORS_FILE) ||
                            fname.equalsIgnoreCase(REPORT_VENDORS) ||
                            fname.equalsIgnoreCase(REPORT_PRODUCTS) ||
                            fname.equalsIgnoreCase(ERRORS_LOG)) {
                        continue;
                    }
                    processSalesFile(file, vendors, products, vendorTotals, productQuantities, errors);
                }
            }

            writeVendorReport(cwd.resolve(REPORT_VENDORS), vendorTotals, vendors);
            writeProductReport(cwd.resolve(REPORT_PRODUCTS), productQuantities, products);

            if (!errors.isEmpty()) {
                Files.write(cwd.resolve(ERRORS_LOG), errors, ENCODING, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                System.out.println("Se generó " + ERRORS_LOG + " con " + errors.size() + " entradas.");
            }

            System.out.println("Procesamiento finalizado correctamente. Archivos generados:");
            System.out.println(" - " + REPORT_VENDORS);
            System.out.println(" - " + REPORT_PRODUCTS);
            System.out.println("=== Fin ===");
        } catch (Exception e) {
            System.err.println("Error crítico durante el procesamiento: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Map<String, Product> loadProducts(Path p) throws IOException {
        Map<String, Product> map = new HashMap<>();
        if (!Files.exists(p)) {
            throw new IOException("No se encontró el archivo de productos: " + p.toString());
        }
        try (BufferedReader br = Files.newBufferedReader(p, ENCODING)) {
            String line;
            int lineNo = 0;
            while ((line = br.readLine()) != null) {
                lineNo++;
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(";");
                if (parts.length < 3) {
                    System.err.println("productos.csv: línea " + lineNo + " con formato inválido, se ignora.");
                    continue;
                }
                String id = parts[0].trim();
                String name = parts[1].trim();
                String priceStr = parts[2].trim();
                try {
                    double price = Double.parseDouble(priceStr);
                    if (price < 0) {
                        System.err.println("productos.csv: precio negativo en línea " + lineNo + " (producto " + id + "), se ignora.");
                        continue;
                    }
                    map.put(id, new Product(id, name, price));
                } catch (NumberFormatException nfe) {
                    System.err.println("productos.csv: precio inválido en línea " + lineNo + " -> " + priceStr);
                }
            }
        }
        return map;
    }

    private static Map<String, Vendor> loadVendors(Path p) throws IOException {
        Map<String, Vendor> map = new HashMap<>();
        if (!Files.exists(p)) {
            throw new IOException("No se encontró el archivo de vendedores: " + p.toString());
        }
        try (BufferedReader br = Files.newBufferedReader(p, ENCODING)) {
            String line;
            int lineNo = 0;
            while ((line = br.readLine()) != null) {
                lineNo++;
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(";");
                if (parts.length < 4) {
                    System.err.println("vendedores.csv: línea " + lineNo + " con formato inválido, se ignora.");
                    continue;
                }
                String tipo = parts[0].trim();
                String numero = parts[1].trim();
                String nombres = parts[2].trim();
                String apellidos = parts[3].trim();
                String key = vendorKey(tipo, numero);
                map.put(key, new Vendor(tipo, numero, nombres, apellidos));
            }
        }
        return map;
    }

    private static void processSalesFile(Path file,
                                         Map<String, Vendor> vendors,
                                         Map<String, Product> products,
                                         Map<String, Double> vendorTotals,
                                         Map<String, Integer> productQuantities,
                                         List<String> errors) {
        try (BufferedReader br = Files.newBufferedReader(file, ENCODING)) {
            String fname = file.getFileName().toString();
            String line;
            String detectedTipo = null;
            String detectedNumero = null;
            List<String> lines = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) lines.add(line.trim());
            }
            if (lines.isEmpty()) {
                errors.add("Archivo vacío: " + fname);
                return;
            }

            String first = lines.get(0);
            String[] firstTokens = first.split(";");
            int startIndex = 0;
            if (firstTokens.length >= 2 && firstTokens[0].trim().length() > 0 && firstTokens[1].trim().length() > 0) {
                detectedTipo = firstTokens[0].trim();
                detectedNumero = firstTokens[1].trim();
                startIndex = 1;
            } else {
                Pattern p = Pattern.compile("vendedor[_-]?(\\d+)", Pattern.CASE_INSENSITIVE);
                Matcher m = p.matcher(fname);
                if (m.find()) {
                    detectedNumero = m.group(1);
                    String foundKey = findVendorKeyByNumber(vendors, detectedNumero);
                    if (foundKey != null) {
                        String[] keyParts = foundKey.split(";", 2);
                        detectedTipo = keyParts[0];
                    }
                } else {
                    Pattern p2 = Pattern.compile("(\\d+)");
                    Matcher m2 = p2.matcher(fname);
                    if (m2.find()) {
                        detectedNumero = m2.group(1);
                        String foundKey = findVendorKeyByNumber(vendors, detectedNumero);
                        if (foundKey != null) {
                            String[] keyParts = foundKey.split(";", 2);
                            detectedTipo = keyParts[0];
                        }
                    }
                }
            }

            String vendorKey;
            if (detectedTipo != null && detectedNumero != null) {
                vendorKey = vendorKey(detectedTipo, detectedNumero);
            } else if (detectedNumero != null) {
                vendorKey = vendorKey("UNK", detectedNumero);
            } else {
                vendorKey = "UNK;UNKNOWN";
                errors.add("No se pudo identificar vendedor del archivo: " + fname);
            }

            for (int i = startIndex; i < lines.size(); i++) {
                String l = lines.get(i);
                String[] tok = l.split(";");
                if (tok.length < 2) {
                    errors.add("Formato inválido en archivo " + fname + " línea: " + l);
                    continue;
                }
                String prodId = tok[0].trim();
                String qtyStr = tok[1].trim();
                if (prodId.isEmpty() || qtyStr.isEmpty()) {
                    errors.add("Campo vacío en archivo " + fname + " línea: " + l);
                    continue;
                }
                int qty;
                try {
                    qty = Integer.parseInt(qtyStr);
                } catch (NumberFormatException nfe) {
                    errors.add("Cantidad inválida en " + fname + " línea: " + l);
                    continue;
                }
                if (qty < 0) {
                    errors.add("Cantidad negativa en " + fname + " línea: " + l);
                    continue;
                }
                Product prod = products.get(prodId);
                if (prod == null) {
                    errors.add("Producto no encontrado (id=" + prodId + ") en archivo " + fname + " línea: " + l);
                    continue;
                }
                double addition = prod.price * qty;
                vendorTotals.put(vendorKey, vendorTotals.getOrDefault(vendorKey, 0.0) + addition);
                productQuantities.put(prodId, productQuantities.getOrDefault(prodId, 0) + qty);
            }
        } catch (IOException ioe) {
            errors.add("IOException leyendo archivo " + file.getFileName().toString() + " -> " + ioe.getMessage());
        }
    }

    private static void writeVendorReport(Path out,
                                          Map<String, Double> vendorTotals,
                                          Map<String, Vendor> vendors) throws IOException {
        List<Entry<String, Double>> list = new ArrayList<>(vendorTotals.entrySet());
        Collections.sort(list, new Comparator<Entry<String, Double>>() {
            @Override
            public int compare(Entry<String, Double> e1, Entry<String, Double> e2) {
                return Double.compare(e2.getValue(), e1.getValue());
            }
        });

        DecimalFormat df = new DecimalFormat("#0.00");

        try (BufferedWriter bw = Files.newBufferedWriter(out, ENCODING, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            bw.write("Monto;TipoDocumento;NumeroDocumento;Nombres;Apellidos");
            bw.newLine();
            for (Entry<String, Double> e : list) {
                String key = e.getKey();
                double total = e.getValue();
                Vendor v = vendors.get(key);
                String tipo = "UNK";
                String numero = "";
                String nombres = "";
                String apellidos = "";
                if (v != null) {
                    tipo = v.tipoDocumento;
                    numero = v.numeroDocumento;
                    nombres = v.nombres;
                    apellidos = v.apellidos;
                } else {
                    String[] parts = key.split(";", 2);
                    if (parts.length == 2) {
                        tipo = parts[0];
                        numero = parts[1];
                    }
                }
                bw.write(df.format(total) + ";" + tipo + ";" + numero + ";" + nombres + ";" + apellidos);
                bw.newLine();
            }
        }
    }

    private static void writeProductReport(Path out,
                                           Map<String, Integer> productQuantities,
                                           Map<String, Product> products) throws IOException {
        List<Entry<String, Integer>> list = new ArrayList<>(productQuantities.entrySet());
        Collections.sort(list, new Comparator<Entry<String, Integer>>() {
            @Override
            public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
                return Integer.compare(e2.getValue(), e1.getValue());
            }
        });

        DecimalFormat df = new DecimalFormat("#0.00");

        try (BufferedWriter bw = Files.newBufferedWriter(out, ENCODING, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            bw.write("Nombre;Precio;CantidadVendida");
            bw.newLine();
            for (Entry<String, Integer> e : list) {
                String pid = e.getKey();
                int qty = e.getValue();
                Product prod = products.get(pid);
                if (prod == null) {
                    bw.write("UNKNOWN;" + "0.00" + ";" + qty);
                } else {
                    bw.write(prod.name + ";" + df.format(prod.price) + ";" + qty);
                }
                bw.newLine();
            }
        }
    }

    private static String vendorKey(String tipo, String numero) {
        return tipo + ";" + numero;
    }

    private static String findVendorKeyByNumber(Map<String, Vendor> vendors, String numero) {
        for (String key : vendors.keySet()) {
            String[] parts = key.split(";", 2);
            if (parts.length >= 2 && parts[1].equals(numero)) return key;
        }
        return null;
    }

    private static class Product {
        final String id;
        final String name;
        final double price;

        Product(String id, String name, double price) {
            this.id = id;
            this.name = name;
            this.price = price;
        }
    }

    private static class Vendor {
        final String tipoDocumento;
        final String numeroDocumento;
        final String nombres;
        final String apellidos;

        Vendor(String tipoDocumento, String numeroDocumento, String nombres, String apellidos) {
            this.tipoDocumento = tipoDocumento;
            this.numeroDocumento = numeroDocumento;
            this.nombres = nombres;
            this.apellidos = apellidos;
        }
    }
}
