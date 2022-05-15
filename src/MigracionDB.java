import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Importante, antes de realizar el script
 * Se requiere convertir las columnas ESTADO y EXPEDIDO a tipo de dato: TEXT 15, con el programa dbvisualizer plus. La tabla: SATNOMBR
 * Eliminar columna OBS de la tabla SATROL, y eliminar el archivo nuevo creado SATTROL_2
 */
public class MigracionDB {
    private static String ROOT_DIR ="c:\\siim\\";
    private static String TABLA_PERMITIDA[] = {"sat_acep","sat_d_in","sat_e_pr","sat_hori","sat_m_vi","sat_p_in","sat_unif","satactis","sataux","satbarri","satciiu",
            "satcodac","satconst","satfecha","satgloba","satgm","satinmus","satliqin","satnombr","satporce","satrecin","satrecur","satresto",
            "sattopes","sattrol","satusrid","satvenac","satvenin","satzonas"};
    public static void main(String[] args) throws Exception{
        ejm1();
    }
    public static void ejm1() throws Exception{
        try {
            SimpleDateFormat formato = new SimpleDateFormat("DDMMYY_hhmmss");
            String nameFile = ROOT_DIR + "BdSiim_"+formato.format(new Date())+".sql";
            createFile(nameFile);
            StringBuilder sql = new StringBuilder();
            List<String> tablas = getNameTablaDirectory(ROOT_DIR);
            if(tablas != null && !tablas.isEmpty()) {
                String valor = "";
                for(String tabla : tablas) {
                    if(estaPermitido(tabla)) {
                        InputStream inputStream = new FileInputStream(ROOT_DIR+tabla);
                        DBFReader reader = new DBFReader(inputStream);
                        int numColumn = reader.getFieldCount();

                        sql.append("create table "+tabla.replace(".DBF","").toLowerCase()+"(");
                        boolean existField = false;
                        byte fieldType []= new byte[numColumn];
                        for (int i = 0; i < numColumn; i++) {
                            DBFField field = reader.getField(i);
                            DBFDataType dataType = field.getType();
                            fieldType[i] = dataType.getCode();
                            String nameField = getField(field.getName());
                            generateColumn(sql, dataType , field,nameField,numColumn,i);
                        }
                        sql.append(");\n");
                        System.out.println(tabla);
                        int numberRow = reader.getRecordCount();
                        if(numberRow > 0) {
                            sql.append("insert into "+tabla.replace(".DBF","").toLowerCase()+" values");
                            Object[] rowObjects;
                            int fila = 0;
                            while ((rowObjects = reader.nextRecord()) != null) {
                                insertRow(sql,numColumn,fieldType, rowObjects,valor);
                                fila++;
                                if(fila < numberRow) {
                                    sql.append("),\n");
                                } else {
                                    sql.append(");\n");
                                }
                            }
                        }
                        inputStream.close();
                    }
                }
                FileWriter myWriter = new FileWriter(nameFile);
                myWriter.write(sql.toString());
                myWriter.close();
            }

        } catch (Exception e) {
            System.out.println("Error while reading user files");
            e.printStackTrace();
        }
    }
    public static List<String> getNameTablaDirectory(String directory) {
        List<String> tablas = new ArrayList<>();
        File folder = new File(directory);
        for (File file : folder.listFiles()) {
            if (!file.isDirectory()) {
                if(file.getName().endsWith(".DBF")){
                    //Tabla
                    tablas.add(file.getName());
                }
            }
        }
        return tablas;
    }
    public static void createFile(String nameFile) {
        try {
            File myObj = new File(nameFile);
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
    public static boolean estaPermitido(String tabla) {
        boolean esPermitido = false;
        for (int i = 0; i < TABLA_PERMITIDA.length; i++) {
            if(TABLA_PERMITIDA[i].equals(tabla.replace(".DBF","").toLowerCase())) {
                esPermitido = true;
            }
        }
        return esPermitido;
    }
    public static String getField(String field) {
        String nameField = field.toLowerCase();
        if(nameField.equals("user")) {
            nameField = "\"user\"";
        }
        if(nameField.equals("date")) {
            nameField = "\"date\"";
        }
        if(nameField.equals("in")) {
            nameField = "\"in\"";
        }
        return nameField;
    }
    public static void generateColumn(StringBuilder sql, DBFDataType dataType , DBFField field,String nameField,int numColumn,int i){
        boolean existField = false;
        if(DBFDataType.CHARACTER.getCode() == dataType.getCode()) {
            existField = true;
            sql.append(nameField+" "+"varchar("+field.getLength()+")");
        } else {
            if(DBFDataType.DATE.getCode() == dataType.getCode()) {
                existField = true;
                sql.append(nameField+" "+"date");
            } else {
                if(DBFDataType.NUMERIC.getCode() == dataType.getCode()) {
                    existField = true;
                    sql.append(nameField+" "+"numeric("+dataType.getMaxSize()+","+field.getDecimalCount()+")");
                } else {
                    if(DBFDataType.LOGICAL.getCode() == dataType.getCode()) {
                        existField = true;
                        sql.append(nameField+" "+"boolean");
                    } else {
//                        System.out.println("no se encontro"+field.getName()+"____"+ dataType.name()+"-->"+tabla);
                        sql.deleteCharAt(sql.length()-1);
                    }

                }
            }
        }
        if(numColumn > 1 && existField){
            if(i<(numColumn-1)){
                sql.append(",");
            }
        }
    }
    public static void insertRow(StringBuilder sql, int numColumn, byte fieldType[], Object rowObjects[],String valor){
        for (int i = 0; i < numColumn; i++) {
//                                valor = ""+rowObjects[i]+"";
            if(DBFDataType.CHARACTER.getCode() == fieldType[i]) {
                if((""+rowObjects[i]).equals("null")) {
                    valor = ""+rowObjects[i];
                } else {
                    valor = (""+rowObjects[i]).replaceAll("\"","");
                    valor = valor.replaceAll("\'","");
                    valor = "\'"+valor+"\'";
                }

            } else {
                if(DBFDataType.DATE.getCode() == fieldType[i]) {
                    if((""+rowObjects[i]).equals("null")) {
                        valor = ""+rowObjects[i];
                    } else {
                        valor = "to_date(\'"+rowObjects[i]+"\',\'Dy Mon DD HH24:MI:SS VET YYYY\')";
                    }
                } else {
                    if(DBFDataType.NUMERIC.getCode() == fieldType[i]) {
                        valor = ""+(rowObjects[i]!=null?rowObjects[i]:0)+"";
                    } else {
                        if(DBFDataType.LOGICAL.getCode() == fieldType[i]) {
                            valor = ""+rowObjects[i]+"";
                        }
                    }
                }
            }
            if(i==0) {
                sql.append("(");
            }
            sql.append(valor);
            if(numColumn > 1) {
                if(i<(numColumn-1)){
                    sql.append(",");
                }
            }
        }
    }
}
