package fr.devinci.bigdata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * MockDataGenerator - Generates synthetic CSV data for testing.
 *
 * Creates realistic French address data with:
 * - Unique IDs
 * - French department codes
 * - Street names and numbers
 * - Postal codes and communes
 * - Controllable changes between days
 */
public class MockDataGenerator {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final Random random = new Random(42); // Fixed seed for reproducibility

    private static final String[] STREET_TYPES = {
            "Rue", "Avenue", "Boulevard", "Place", "Allée", "Chemin", "Impasse", "Route"
    };

    private static final String[] STREET_NAMES = {
            "de la République", "Victor Hugo", "Jean Jaurès", "de la Liberté",
            "de la Paix", "du Général de Gaulle", "de Paris", "de la Mairie",
            "Nationale", "des Écoles", "du Commerce", "de l'Église", "du Marché",
            "de la Gare", "des Fleurs", "du Château", "de Verdun", "de la Poste"
    };

    private static final String[] COMMUNES = {
            "Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg",
            "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre",
            "Saint-Étienne", "Toulon", "Grenoble", "Dijon", "Angers", "Nîmes", "Villeurbanne"
    };

    private static final String[] DEPARTMENTS = {
            "75", "13", "69", "31", "06", "44", "67", "34", "33", "59",
            "35", "51", "76", "42", "83", "38", "21", "49", "30", "92"
    };

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: MockDataGenerator <outputDir> <startDate> <numDays>");
            System.err.println("Example: MockDataGenerator /tmp/data 2025-01-01 50");
            System.exit(1);
        }

        String outputDir = args[0];
        String startDateStr = args[1];
        int numDays = Integer.parseInt(args[2]);

        LocalDate startDate = LocalDate.parse(startDateStr, DATE_FORMAT);
        new File(outputDir).mkdirs();

        System.out.println("Generating " + numDays + " days of mock data starting from " + startDateStr);
        System.out.println("Output directory: " + outputDir);

        List<Address> addresses = generateInitialDataset(10000);

        for (int day = 0; day < numDays; day++) {
            LocalDate currentDate = startDate.plusDays(day);
            String dateStr = currentDate.format(DATE_FORMAT);

            if (day > 0) {
                // Apply changes for subsequent days
                addresses = applyDailyChanges(addresses, day);
            }

            String filename = outputDir + "/dump-" + dateStr + ".csv";
            writeCSV(addresses, filename);
            System.out.println("Generated: " + filename + " (" + addresses.size() + " records)");
        }

        System.out.println("Mock data generation completed!");
    }

    private static List<Address> generateInitialDataset(int count) {
        List<Address> addresses = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            addresses.add(generateRandomAddress("ID" + String.format("%08d", i)));
        }
        return addresses;
    }

    private static Address generateRandomAddress(String id) {
        String numero = String.valueOf(random.nextInt(200) + 1);
        String suffixe = random.nextInt(10) < 2 ? "BIS" : "";
        String nomVoie = STREET_TYPES[random.nextInt(STREET_TYPES.length)] + " " +
                STREET_NAMES[random.nextInt(STREET_NAMES.length)];
        String codePostal = DEPARTMENTS[random.nextInt(DEPARTMENTS.length)] +
                String.format("%03d", random.nextInt(1000));
        String nomCommune = COMMUNES[random.nextInt(COMMUNES.length)];
        String codeDepartement = DEPARTMENTS[random.nextInt(DEPARTMENTS.length)];
        String latitude = String.format("%.6f", 43.0 + random.nextDouble() * 8);
        String longitude = String.format("%.6f", -4.0 + random.nextDouble() * 12);
        return new Address(id, numero, suffixe, nomVoie, codePostal,
                nomCommune, codeDepartement, latitude, longitude);
    }

    private static List<Address> applyDailyChanges(List<Address> addresses, int dayNumber) {
        List<Address> result = new ArrayList<>(addresses);

        // Change rates increase slightly over time
        double deleteRate = Math.min(0.002 * (1 + dayNumber / 50.0), 0.01);
        double addRate = Math.min(0.003 * (1 + dayNumber / 50.0), 0.015);
        double modifyRate = Math.min(0.005 * (1 + dayNumber / 50.0), 0.02);

        // Delete some addresses
        int deleteCount = (int) (result.size() * deleteRate);
        for (int i = 0; i < deleteCount && !result.isEmpty(); i++) {
            result.remove(random.nextInt(result.size()));
        }

        // Add new addresses
        int addCount = (int) (result.size() * addRate);
        int nextId = result.stream()
                .mapToInt(a -> Integer.parseInt(a.id.substring(2)))
                .max()
                .orElse(0) + 1;
        for (int i = 0; i < addCount; i++) {
            result.add(generateRandomAddress("ID" + String.format("%08d", nextId++)));
        }

        // Modify some addresses
        int modifyCount = (int) (result.size() * modifyRate);
        for (int i = 0; i < modifyCount && !result.isEmpty(); i++) {
            int idx = random.nextInt(result.size());
            Address old = result.get(idx);

            int field = random.nextInt(4);
            switch (field) {
                case 0: // Change number
                    result.set(idx, new Address(
                            old.id,
                            String.valueOf(random.nextInt(200) + 1),
                            old.suffixe, old.nomVoie, old.codePostal,
                            old.nomCommune, old.codeDepartement,
                            old.latitude, old.longitude
                    ));
                    break;
                case 1: // Change commune
                    result.set(idx, new Address(
                            old.id, old.numero, old.suffixe, old.nomVoie,
                            old.codePostal, COMMUNES[random.nextInt(COMMUNES.length)],
                            old.codeDepartement, old.latitude, old.longitude
                    ));
                    break;
                case 2: // Change coordinates
                    result.set(idx, new Address(
                            old.id, old.numero, old.suffixe, old.nomVoie,
                            old.codePostal, old.nomCommune, old.codeDepartement,
                            String.format("%.6f", 43.0 + random.nextDouble() * 8),
                            String.format("%.6f", -4.0 + random.nextDouble() * 12)
                    ));
                    break;
                case 3: // Change street name
                    result.set(idx, new Address(
                            old.id, old.numero, old.suffixe,
                            STREET_TYPES[random.nextInt(STREET_TYPES.length)] + " " +
                                    STREET_NAMES[random.nextInt(STREET_NAMES.length)],
                            old.codePostal, old.nomCommune, old.codeDepartement,
                            old.latitude, old.longitude
                    ));
                    break;
                default:
                    break;
            }
        }

        return result;
    }

    private static void writeCSV(List<Address> addresses, String filename) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            // Header
            writer.write("id,numero,suffixe,nom_voie,code_postal,nom_commune,code_departement,latitude,longitude\n");
            // Data
            for (Address addr : addresses) {
                writer.write(addr.toCSV() + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing CSV file: " + filename, e);
        }
    }

    static class Address {
        String id;
        String numero;
        String suffixe;
        String nomVoie;
        String codePostal;
        String nomCommune;
        String codeDepartement;
        String latitude;
        String longitude;

        Address(String id, String numero, String suffixe, String nomVoie,
                String codePostal, String nomCommune, String codeDepartement,
                String latitude, String longitude) {
            this.id = id;
            this.numero = numero;
            this.suffixe = suffixe;
            this.nomVoie = nomVoie;
            this.codePostal = codePostal;
            this.nomCommune = nomCommune;
            this.codeDepartement = codeDepartement;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        String toCSV() {
            return String.join(",",
                    id, numero, suffixe, nomVoie, codePostal, nomCommune,
                    codeDepartement, latitude, longitude);
        }
    }
}
