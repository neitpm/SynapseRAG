import sys
import json
from pathlib import Path
import wikipediaapi

# Đảm bảo console Windows hiển thị tiếng Việt chuẩn
sys.stdout.reconfigure(encoding="utf-8")

def crawl_and_save_locally(title: str, language: str = "vi"):
    print("=" * 70)
    print(f"📥 BẮT ĐẦU CÀO BÀI VIẾT: '{title}' ({language.upper()}) VÀ LƯU VÀO ĐĨA")
    print("   (Hoạt động độc lập 100%, KHÔNG kết nối Kafka, KHÔNG gửi mạng nội bộ)")
    print("=" * 70)

    # 1. Gọi Wikipedia API
    wiki = wikipediaapi.Wikipedia(
        user_agent="SynapseRAG-Downloader/1.0 (contact@synapserag.local)",
        language=language,
        extract_format=wikipediaapi.ExtractFormat.WIKI
    )
    page = wiki.page(title)

    if not page.exists():
        print(f"❌ THẤT BẠI: Không tìm thấy bài viết '{title}' trên Wikipedia ({language})!")
        return

    # 2. Tạo thư mục lưu trữ cục bộ
    output_dir = Path(__file__).resolve().parent.parent.parent / "crawled_samples"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Đặt tên file an toàn (loại bỏ ký tự đặc biệt)
    safe_name = "".join([c if c.isalnum() or c in (" ", "_", "-") else "_" for c in title]).strip()
    txt_path = output_dir / f"{safe_name}.txt"
    json_path = output_dir / f"{safe_name}.json"

    # 3. Ghi ra file Text thuần
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"TIÊU ĐỀ: {page.title}\n")
        f.write(f"URL: {page.fullurl}\n")
        f.write(f"PAGE ID: {page.pageid}\n")
        f.write("=" * 70 + "\n\n")
        f.write(page.text)

    # 4. Ghi ra file JSON có cấu trúc (gồm tóm tắt và categories)
    data = {
        "page_id": page.pageid,
        "title": page.title,
        "url": page.fullurl,
        "char_count": len(page.text),
        "summary": page.summary,
        "categories": [c.replace("Thể loại:", "").replace("Category:", "") for c in list(page.categories.keys())],
        "content": page.text
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("✅ CÀO VÀ LƯU THÀNH CÔNG!")
    print(f"   • Page ID          : {page.pageid}")
    print(f"   • Tổng số ký tự    : {len(page.text):,} ký tự")
    print(f"   • File Text đã lưu : {txt_path}")
    print(f"   • File JSON đã lưu : {json_path}")
    print("\n💡 Bạn có thể mở trực tiếp 2 file trên để đọc và kiểm tra nội dung bài viết!")
    print("=" * 70)

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "Trí tuệ nhân tạo"
    lang = sys.argv[2] if len(sys.argv) > 2 else "vi"
    crawl_and_save_locally(target, lang)
