import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def normalize_industries(industries: str):
    industry_list = industries.split(', ')

    # Advertising and marketing - Quảng cáo và tiếp thị
    # Aerospace - Hàng không
    # Agriculture - Nông nghiệp
    # Computer and technology - IT
    # Construction - Xây dựng
    # Education - Giáo dục
    # Energy - Năng lượng
    # Entertainment - Giải trí
    # Fashion - Thời trang
    # Finance and economic - Tài chính và kinh tế
    # Food and beverage - Thực phẩm và đồ uống
    # Health care - Chăm sóc sức khỏe
    # Hospitality - Dịch vụ
    # Manufacturing - Sản xuất
    # Media and news - Truyền thông và tin tức
    # Mining - Khai thác mỏ
    # Pharmaceutical - Dược phẩm
    # Telecommunication - Viễn thông
    # Transportation - Vận tải

    if "Kinh doanh / Bán hàng" in industry_list[0] :
        return "Finance and economic"
    if "Kế toán / Kiểm toán" in industry_list[0] :
        return "Finance and economic"
    if "Ngân hàng / Tài chính" in industry_list[0] :
        return "Finance and economic"
    if "Tài chính / Đầu tư" in industry_list[0] :
        return "Finance and economic"
    if "Việc làm IT" in industry_list[0] :
        return "Computer and technology"
    if "IT phần mềm" in industry_list[0] :
        return "Computer and technology"
    if "Công nghệ thông tin" in industry_list[0] :
        return "Computer and technology"
    if "IT Phần cứng / Mạng" in industry_list[0] :
        return "Computer and technology"  
    if "Marketing / Truyền thông / Quảng cáo" in industry_list[0] :
        return "Advertising and marketing"
    if "Xây dựng" in industry_list[0] :
        return "Construction"
    if "Hành chính / Văn phòng" in industry_list[0] :
        return "Other" 
    if "Quản lý điều hành" in industry_list[0] :
        return "Other"
    if "Dịch vụ khách hàng" in industry_list[0] :
        return "Hospitality"
    if "Nhân sự" in industry_list[0] :
        return "Other"
    if "Điện / Điện tử / Điện lạnh" in industry_list[0] :
        return "Manufacturing" 
    if "Bất động sản" in industry_list[0] :
        return "Construction"
    if "Sản xuất" in industry_list[0] :
        return "Manufacturing"
    if "Giáo dục / Đào tạo" in industry_list[0] :
        return "Education"
    if "Bán lẻ / bán sỉ" in industry_list[0] :
        return "Finance and economic"
    if "Thực phẩm / Đồ uống" in industry_list[0] :
        return "Food and beverage"
    if "Khách sạn / Nhà hàng" in industry_list[0] :
        return "Hospitality"
    if "Cơ khí / Chế tạo / Tự động hóa" in industry_list[0] :
        return "Manufacturing"
    if "Ngành nghề khác" in industry_list[0] :
        return "Other"
    if "Vận tải / Kho vận" in industry_list[0] :
        return "Transportation"
    if "Logistics" in industry_list[0] :
        return "Transportation"
    if "Luật/Pháp lý" in industry_list[0] :
        return "Other"
    if "Xuất nhập khẩu" in industry_list[0] :
        return "Transportation"
    if "Y tế / Dược" in industry_list[0] :
        return "Pharmaceutical"
    if "Quản lý chất lượng (QA/QC)" in industry_list[0] :
        return "Other"
    if "Báo chí / Truyền hình" in industry_list[0] :
        return "Media and news"
    if "Hoạch định/Dự án" in industry_list[0] :
        return "Finance and economic"
    if "Tư vấn" in industry_list[0] :
        return "Hospitality"
    if "Du lịch" in industry_list[0] :
        return "Hospitality"
    if "Công nghệ cao" in industry_list[0] :
        return "Computer and technology"
    if "Kiến trúc" in industry_list[0] :
        return "Construction"
    if "Thiết kế đồ họa" in industry_list[0] :
        return "Computer and technology"
    if "Thiết kế nội thất" in industry_list[0] :
        return "Construction"
    if "Điện tử viễn thông" in industry_list[0] :
        return "Telecommunication"
    if "Bảo trì / Sửa chữa" in industry_list[0] :
        return "Manufacturing"
    if "Dược phẩm / Công nghệ sinh học" in industry_list[0] :
        return "Pharmaceutical"
    if "Thư ký / Trợ lý" in industry_list[0] :
        return "Other"
    if "Chứng khoán / Vàng / Ngoại tệ" in industry_list[0] :
        return "Finance and economic"
    if "Thời trang" in industry_list[0] :
        return "Fashion"
    if "Công nghệ Ô tô" in industry_list[0] :
        return "Manufacturing"
    if "Mỹ phẩm / Trang sức" in industry_list[0] :
        return "Fashion"
    if "Biên / Phiên dịch" in industry_list[0] :
        return "Other"
    if "Hàng tiêu dùng" in industry_list[0] :
        return "Manufacturing"
    if "Dệt may / Da giày" in industry_list[0] :
        return "Manufacturing"
    if "Bán hàng kỹ thuật" in industry_list[0] :
        return "Finance and economic"
    if "Spa / Làm đẹp" in industry_list[0] :
        return "Health care"
    if "Tổ chức sự kiện / Quà tặng" in industry_list[0] :
        return "Computer and technology"
    if "Tổ chức sự kiện / Quà tặng" in industry_list[0] :
        return "Hospitality"
    if "Nông / Lâm / Ngư nghiệp" in industry_list[0] :
        return "Agriculture"
    if "Môi trường / Xử lý chất thải" in industry_list[0] :
        return "Agriculture"
    if "Mỹ thuật / Nghệ thuật / Điện ảnh" in industry_list[0] :
        return "Entertainment"
    if "Bảo hiểm" in industry_list[0] :
        return "Hospitality"
    if "Hoá học / Sinh học" in industry_list[0] :
        return "Pharmaceutical"
    if "Sản phẩm công nghiệp" in industry_list[0] :
        return "Mining"
    if "Bưu chính - Viễn thông" in industry_list[0] :
        return "Telecommunication"
    if "Hàng cao cấp" in industry_list[0] :
        return "Manufacturing"
    if "Hàng gia dụng" in industry_list[0] :
        return "Manufacturing"
    if "An toàn lao động" in industry_list[0] :
        return "Other"
    if "Dầu khí/Hóa chất" in industry_list[0] :
        return "Mining"
    if "Địa chất / Khoáng sản" in industry_list[0] :
        return "Mining"
    if "In ấn / Xuất bản" in industry_list[0] :
        return "Education"
    if "Hàng hải" in industry_list[0] :
        return "Transportation"
    if "NGO / Phi chính phủ / Phi lợi nhuận" in industry_list[0] :
        return "Other"
    if "Hàng không" in industry_list[0] :
        return "Transportation"

    
    return "Other"
