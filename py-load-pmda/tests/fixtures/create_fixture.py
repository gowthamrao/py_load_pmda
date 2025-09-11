import zipfile
from pathlib import Path

import pandas as pd


def create_jader_test_fixture() -> None:
    """
    Creates a test JADER zip file with correctly encoded Shift-JIS CSVs.
    """
    fixtures_dir = Path(__file__).parent
    output_zip_path = fixtures_dir / "test_jader_pipeline.zip"

    # Data for the four CSV files
    demo_data = {
        "識別番号": [1],
        "性別": ["男性"],
        "年齢": ["30代"],
        "体重": ["60"],
        "身長": ["170"],
        "報告年度・四半期": ["202501"],
        "転帰": ["回復"],
        "報告区分": ["企業"],
        "報告者職種": ["医師"],
    }
    drug_data = {
        "識別番号": [1],
        "医薬品の関与": ["被疑薬"],
        "医薬品名": ["テストドラッグ"],
        "使用理由": ["頭痛"],
    }
    reac_data = {"識別番号": [1], "副作用名": ["めまい"], "発現日": ["20250101"]}
    hist_data = {"識別番号": [1], "原疾患等": ["高血圧"]}

    data_map = {
        "DEMO.csv": demo_data,
        "DRUG.csv": drug_data,
        "REAC.csv": reac_data,
        "HIST.csv": hist_data,
    }

    # Create a zip file and write each CSV into it with Shift-JIS encoding
    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, data in data_map.items():
            df = pd.DataFrame(data)
            # Use pandas to_csv with the correct encoding
            csv_bytes = df.to_csv(index=False, encoding="shift_jis").encode("shift_jis")
            zf.writestr(filename, csv_bytes)

    print(f"Successfully created test fixture: {output_zip_path}")


if __name__ == "__main__":
    create_jader_test_fixture()
