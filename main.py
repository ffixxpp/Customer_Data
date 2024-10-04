import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl


mpl.rc('font', family='Kanit')


class DataLoader:
    def __init__(self, base_path="./mnt/data/raw_data/FD/"):
        self.folders = ["CH", "SC", "SR", "TH"]
        self.base_path = base_path
        self.data = {
            "procedurelog": {},
            "payment": {}
        }
        self.processed_data = {
            "procedurelog": {},
            "payment": {}
        }
        self.rfm_data = {
            "procedurelog": {},
            "payment": {}
        }
 # โหลดข้อมูลจากไฟล์ CSV ที่ตั้งอยู่ในหลายๆโฟลเดอร์
    def load_data(self):
        procedurelog_cols = ["PatNum", "ProcDate", "ProcFee"]
        payment_cols = ["PatNum", "PayDate", "ProcFee_amt"]

        for folder in self.folders:
            self.data["procedurelog"][folder] = pd.read_csv(f"{self.base_path}{folder}/procedurelog.csv",
                                                            encoding='ISO-8859-1', low_memory=False,
                                                            usecols=procedurelog_cols)
            # Filter out rows where PatNum is negative
            self.data["procedurelog"][folder] = self.data["procedurelog"][folder][
                self.data["procedurelog"][folder]['PatNum'] >= 0]

            self.data["payment"][folder] = pd.read_csv(f"{self.base_path}{folder}/ttr_payment.csv",
                                                       encoding='ISO-8859-1', low_memory=False, usecols=payment_cols)
            # Filter out rows where PatNum is negative
            self.data["payment"][folder] = self.data["payment"][folder][self.data["payment"][folder]['PatNum'] >= 0]

# ประมวลข้อมูลและจัดรูปแบบให้เหมาะสม
    def process_data(self):
        for folder in self.folders:
            # Process procedurelog
            df = self.data["procedurelog"][folder]
            df['ProcDate'] = pd.to_datetime(df['ProcDate'], dayfirst=True, errors='coerce')
            grouped = df.groupby(['PatNum', 'ProcDate']).agg({'ProcFee': 'sum'}).reset_index()
            grouped = grouped.sort_values(by=['PatNum', 'ProcDate'], ascending=[True, True])
            grouped['year_month_day'] = grouped['ProcDate'].dt.strftime('%d/%m/%Y')
            grouped.drop(columns=['ProcDate'], inplace=True)  # Drop the 'ProcDate' column
            self.processed_data["procedurelog"][folder] = grouped

            # Process payment
            df = self.data["payment"][folder]
            df['PayDate'] = pd.to_datetime(df['PayDate'], dayfirst=True, errors='coerce')
            grouped = df.groupby(['PatNum', 'PayDate']).agg({'ProcFee_amt': 'sum'}).reset_index()
            grouped = grouped.sort_values(by=['PatNum', 'PayDate'], ascending=[True, True])
            grouped['year_month_day'] = grouped['PayDate'].dt.strftime('%d/%m/%Y')
            grouped.drop(columns=['PayDate'], inplace=True)  # Drop the 'PayDate' column
            self.processed_data["payment"][folder] = grouped

# บันทึกข้อมูลที่ประมวลผลแล้วเป็นไฟล์ CSV
    def write_to_csv(self, output_path="./mnt/data/processed_data/"):
        for folder in self.folders:
            procedurelog_folder_path = os.path.join(output_path, folder, "procedurelog_processed")
            payment_folder_path = os.path.join(output_path, folder, "payment_processed")

            # Ensure the directories exist
            os.makedirs(procedurelog_folder_path, exist_ok=True)
            os.makedirs(payment_folder_path, exist_ok=True)

            # Save procedurelog for the entire folder
            self.processed_data["procedurelog"][folder].to_csv(
                f"{procedurelog_folder_path}/{folder}_procedurelog_all_years.csv", index=False
            )

            # Save payment for the entire folder
            self.processed_data["payment"][folder].to_csv(
                f"{payment_folder_path}/{folder}_payment_all_years.csv", index=False
            )

# ฟังก์ชันการคำนวณค่า R F M และการเพิ่มการคำนวณ L  
    def compute_rfm(self):
        for folder in self.folders:
            # Compute RFM for procedurelog
            df_proc = self.data["procedurelog"][folder]
            df_proc['ProcDate'] = pd.to_datetime(df_proc['ProcDate'], dayfirst=True, errors='coerce')
            rfm_proc = df_proc.groupby('PatNum').agg({
                'ProcDate': ['max', 'min'],  # Add 'min' aggregation for calculating 'all day'
                'ProcFee': ['count', 'sum']
            }).reset_index()
            rfm_proc.columns = ['PatNum', 'LastDate', 'FirstDate', 'Count of Orders', 'Sum of Sales']
            rfm_proc['Avg. Sales'] = rfm_proc['Sum of Sales'] / rfm_proc['Count of Orders']
            rfm_proc['all day'] = (rfm_proc['LastDate'] - rfm_proc['FirstDate']).dt.days + 1
            rfm_proc['LastDate'] = rfm_proc['LastDate'].dt.strftime('%d/%m/%Y')
            rfm_proc.drop(columns=['FirstDate'], inplace=True)  # Drop 'FirstDate' column
            self.rfm_data["procedurelog"][folder] = rfm_proc

            # Compute RFM for payment
            df_pay = self.data["payment"][folder]
            df_pay['PayDate'] = pd.to_datetime(df_pay['PayDate'], dayfirst=True, errors='coerce')
            rfm_pay = df_pay.groupby('PatNum').agg({
                'PayDate': ['max', 'min'],  # Add 'min' aggregation for calculating 'all day'
                'ProcFee_amt': ['count', 'sum']
            }).reset_index()
            rfm_pay.columns = ['PatNum', 'LastDate', 'FirstDate', 'Count of Orders', 'Sum of Sales']
            rfm_pay['Avg. Sales'] = rfm_pay['Sum of Sales'] / rfm_pay['Count of Orders']
            rfm_pay['all day'] = (rfm_pay['LastDate'] - rfm_pay['FirstDate']).dt.days + 1
            rfm_pay['LastDate'] = rfm_pay['LastDate'].dt.strftime('%d/%m/%Y')
            rfm_pay.drop(columns=['FirstDate'], inplace=True)  # Drop 'FirstDate' column
            self.rfm_data["payment"][folder] = rfm_pay

    def write_rfm_to_csv(self, output_path="./mnt/data/rfm_data/"):
        for folder in self.folders:
            procedurelog_folder_path = os.path.join(output_path, folder, "procedurelog_rfm")
            payment_folder_path = os.path.join(output_path, folder, "payment_rfm")

            # Ensure the directories exist
            os.makedirs(procedurelog_folder_path, exist_ok=True)
            os.makedirs(payment_folder_path, exist_ok=True)

            # Save RFM for procedurelog
            self.rfm_data["procedurelog"][folder].to_csv(
                f"{procedurelog_folder_path}/{folder}_procedurelog_rfm.csv", index=False
            )

            # Save RFM for payment
            self.rfm_data["payment"][folder].to_csv(
                f"{payment_folder_path}/{folder}_payment_rfm.csv", index=False
            )

    def segment_customers(self):
        segmentation = {
            "procedurelog": {},
            "payment": {}
        }

        for folder in self.folders:
            for category in ["procedurelog", "payment"]:
                df = self.rfm_data[category][folder].copy()

                # Calculate medians for Frequency and Monetary
                median_f = df['Count of Orders'].median()
                median_m = df['Avg. Sales'].median()

                # Create a new column for customer segments
                df['Segment'] = ''

                # High Frequency, High Monetary
                df.loc[(df['Count of Orders'] > median_f) & (df['Avg. Sales'] > median_m), 'Segment'] = 'มาบ่อยจ่ายเยอะ'

                # High Frequency, Low Monetary
                df.loc[(df['Count of Orders'] > median_f) & (
                            df['Avg. Sales'] <= median_m), 'Segment'] = 'มาบ่อยจ่ายไม่เยอะ'

                # Low Frequency, Low Monetary
                df.loc[(df['Count of Orders'] <= median_f) & (
                            df['Avg. Sales'] <= median_m), 'Segment'] = 'มาไม่บ่อยจ่ายไม่เยอะ'

                segmentation[category][folder] = df

        # Optional: display the segmented data
        for folder in self.folders:
            print(f"Folder: {folder}")
            print("\n======= Procedure Log Segmentation =======")
            print(segmentation["procedurelog"][folder].to_string())
            print("\n======= Payment Segmentation =======")
            print(segmentation["payment"][folder].to_string())
            print("\n" + "=" * 40 + "\n")

        return segmentation

# พล็อตกราฟ 3 มิติ แสดงข้อมูลในรูปแบบ 3 มิติ
    def plot_3d_by_segment(self, segmentation, category="procedurelog", save_path='./mnt/data/plots/'):
        for folder in self.folders:
            fig = plt.figure(figsize=(10, 8))
            ax = fig.add_subplot(111, projection='3d')

            colors = {
                'มาบ่อยจ่ายเยอะ': 'g',
                'มาบ่อยจ่ายไม่เยอะ': 'b',
                'มาไม่บ่อยจ่ายไม่เยอะ': 'r'
            }

            df = segmentation[category][folder]
            for segment, color in colors.items():
                segment_df = df[df['Segment'] == segment]
                ax.scatter(segment_df['all day'], segment_df['Count of Orders'], segment_df['Avg. Sales'],
                           c=color, label=segment, s=50, alpha=0.5)

            ax.set_xlabel('All Day (L)')
            ax.set_ylabel('Count of Orders (F)')
            ax.set_zlabel('Avg. Sales (M)')
            ax.set_title(f'3D plot of LRFM for {category} in folder {folder}')
            ax.legend()

            # Save the plot to file
            folder_path = os.path.join(save_path, folder)
            os.makedirs(folder_path, exist_ok=True)
            plt.savefig(os.path.join(folder_path, f'{category}_3d_plot.png'), bbox_inches='tight')
            plt.close()

    def display_data(self):
        for folder in self.folders:
            print(f"Folder: {folder}")
            print("======= Procedure Log Processed =======")
            print(self.processed_data["procedurelog"][folder].to_string())
            print("\n======= Payment Processed =======")
            print(self.processed_data["payment"][folder].to_string())
            print("\n" + "=" * 40 + "\n")


if __name__ == '__main__':
    try:
        loader = DataLoader()
        loader.load_data()
        loader.process_data()
        # loader.display_data()
        loader.write_to_csv()
        loader.compute_rfm()
        segmentation = loader.segment_customers()
        loader.write_rfm_to_csv()
        loader.plot_3d_by_segment(segmentation, "procedurelog")  # Plot and save 3D for 'procedurelog'
        loader.plot_3d_by_segment(segmentation, "payment")  # Plot and save 3D for 'payment'
    except Exception as e:
        print(e)
        
