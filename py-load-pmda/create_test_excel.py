import pandas as pd

df = pd.DataFrame({
    "No.": [1, 2],
    "申請区分": ["区分1", "区分2"],
    "販売名": ["test1", "test2"],
    "一般名": ["generic1", "generic2"],
    "申請者名": ["applicant1", "applicant2"],
    "承認年月日": ["2025/01/01", "2025/01/02"],
    "備考": ["", ""]
})

df.to_excel("tests/fixtures/approvals_2025.xlsx", index=False, sheet_name="Sheet1")
