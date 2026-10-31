import sys
import wikipediaapi

# Đảm bảo terminal Windows hiển thị tiếng Việt UTF-8 chuẩn xác
sys.stdout.reconfigure(encoding="utf-8")

def test_wiki_crawl(title: str, language: str = "vi"):
    print("=" * 70)
    print(f"🌐 ĐANG KIỂM TRA CRAWL WIKIPEDIA: '{title}' ({language.upper()})")
    print("=" * 70)

    wiki = wikipediaapi.Wikipedia(
        user_agent="SynapseRAG-Tester/1.0 (contact@synapserag.local)",
        language=language,
        extract_format=wikipediaapi.ExtractFormat.WIKI
    )

    page = wiki.page(title)

    if not page.exists():
        print(f"❌ THẤT BẠI: Bài viết '{title}' không tồn tại trên Wikipedia ({language})!")
        print("💡 Gợi ý: Hãy kiểm tra lại chính tả hoặc thử tìm kiếm trên Google/Wiki trước.")
        return

    print("✅ CRAWL THÀNH CÔNG! Thông tin trích xuất được:")
    print(f"   • Page ID          : {page.pageid}")
    print(f"   • Tiêu đề chuẩn    : {page.title}")
    print(f"   • Đường dẫn URL    : {page.fullurl}")
    print(f"   • Tổng số ký tự    : {len(page.text):,} ký tự")
    print(f"   • Số danh mục      : {len(page.categories)} categories")
    
    cats = [c.replace("Thể loại:", "").replace("Category:", "") for c in list(page.categories.keys())[:5]]
    print(f"   • Danh mục mẫu     : {', '.join(cats)}")
    
    print("\n--- [TRÍCH ĐOẠN NỘI DUNG 200 KÝ TỰ ĐẦU TIÊN] ---")
    print(page.text[:200].strip())
    print("...\n" + "=" * 70)

if __name__ == "__main__":
    # Đọc tham số từ dòng lệnh nếu có: python test_crawl_standalone.py "Tên bài" "ngôn ngữ"
    if len(sys.argv) > 1:
        article_title = sys.argv[1]
        lang = sys.argv[2] if len(sys.argv) > 2 else "vi"
    else:
        article_title = "Việt Nam"
        lang = "vi"

    test_wiki_crawl(article_title, lang)
