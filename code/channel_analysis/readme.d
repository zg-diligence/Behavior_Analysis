
    目录功能: 以频道|节目为中心分析收视情况

    统计频道:15个央视频道, 32个地方卫视频道

    count_ratings.py: 统计频道收视率
        以小时为单位统计频道收视率, 数据总共28天(缺少5.3|5.23|5.29), 每天24小时
        注意以小时为单位统计频道收视率, 必须将连续一段时间作为整体, 一是因为源数据时间划分不精确, 二是部分用户收视横跨多个小时
        每个频道5月份的收视情况保存在tmp_result/ratings_by_hour文件夹下, 单个文件存储一个频道整个5月份的收视率
        数据存储格式: 5月31天, 共31行, 每天24小时, 每行24个收视率数据
        注意: 5.3|5.23|5.29日没有数据, 但由于后继日期统计也有前一天的收视时间, 故也有极小的收视率, 可视化时忽略这三天的数据

    analyze_ratings.py: 可视化收视率统计结果
        rating_analyze_pic_1:
            卫视top5单日月平均收视率变化曲线
            央视top5单日月平均收视率变化曲线
            总体top10单日月平均收视率变化曲线
            频道top10单日最高收视率对比
            单个频道单日月平均收视率变化曲线图

        rating_analyze_pic_1: (5.6 - 5.19)
            总体top5连续两周收视率变化曲线
            卫视top5连续两周收视率变化曲线
            央视top5连续两周收视率变化曲线
            单个频道连续两周收视率变化曲线

    analyze_programs.py: 统计分析单日节目收视情况
        1.统计单日每个节目的收视时长, 存储tmp_result/prog_times_<day>.txt (不精确, 未考虑后继日期贡献)
        2.生成热门节目词云 tmp_result/prog_cloud_<day>.jpg
        3.统计各个类别节目所占比例, 饼状图可视化 tmp_result/prog_category_<day>.jpg

    tmp_result文件夹下其余文件功能说明:
        channels.txt: 频道映射词典, 央视单个频道可能有多个名字
        nprograms_dict.txt: 节目分类结果词典
        user_ids.txt: all user ids, total 657229

    simhei.ttf 中文字体源
