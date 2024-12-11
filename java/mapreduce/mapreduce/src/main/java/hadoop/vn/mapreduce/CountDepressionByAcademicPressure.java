package hadoop.vn.mapreduce;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class CountDepressionByAcademicPressure {

	public static void main(String[] args) {
		// Đường dẫn tệp CSV (đường dẫn tuyệt đối hoặc tương đối)
		String csvFile = "D:\\hadoop-mapreduce\\java\\mapreduce\\mapreduce\\src\\main\\java\\hadoop\\vn\\mapreduce\\data.csv"; // Đảm bảo đường dẫn chính xác
		try {
			// Đọc tệp CSV
			CSVReader reader = new CSVReader(new FileReader(csvFile));
			String[] line;
			Map<String, int[]> depressionCount = new HashMap<>();

			// Đọc từng dòng dữ liệu từ tệp CSV
			while ((line = reader.readNext()) != null) {
				// Bỏ qua dòng tiêu đề
				if (line[0].equals("Gender")) {
					continue;
				}

				// Trường áp lực học tập và trầm cảm
				String academicPressure = line[2]; // Trường 2 (Mức độ áp lực học tập)
				String depression = line[10]; // Trường 10 (Trạng thái trầm cảm)

				// Khởi tạo bộ đếm cho từng mức độ áp lực học tập
				depressionCount.putIfAbsent(academicPressure, new int[2]);

				// Tăng số lượng trầm cảm hoặc không trầm cảm
				if (depression.equals("Yes")) {
					depressionCount.get(academicPressure)[0]++;
				} else {
					depressionCount.get(academicPressure)[1]++;
				}
			}

			// In kết quả
			for (Map.Entry<String, int[]> entry : depressionCount.entrySet()) {
				String academicPressure = entry.getKey();
				int[] counts = entry.getValue();
				System.out.println("Academic Pressure: " + academicPressure);
				System.out.println("Depression: " + counts[0] + ", No Depression: " + counts[1]);
			}

			// Đóng reader
			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }
}
