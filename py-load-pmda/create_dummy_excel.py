import pandas as pd

# Create a dummy DataFrame that matches the expected structure
data = {
    '承認番号': ['(302AMX00001000)'],
    '申請区分': ['新医療機器'],
    '販売名': ['テストメディカル'],
    '一般名': ['テストジェネリック'],
    '申請者': ['テスト製薬株式会社'],
    '承認日': ['令和7年4月1日'],
    '効能・効果': ['テストの効能'],
    '審査報告書': ['https://www.pmda.go.jp/files/000276011.pdf'],
}
df = pd.DataFrame(data)

# Save the DataFrame to an Excel file
df.to_excel("tests/fixtures/approvals_2025.xlsx", index=False)
