package top10

/**
 * Created by wang on 2020/12/14.
 */
//计算页面单跳转化率，什么是页面单跳转换率，比如一个用户在一次 Session 过程中
//访问的页面路径 3,5,7,9,10,21，那么页面 3 跳到页面 5 叫一次单跳，7-9 也叫一次单跳，
//那么单跳转化率就是要统计页面点击的概率。 比如：计算 3-5 的单跳转化率，先获取符合条件的 Session 对于页面 3 的访问次数（PV）
//为 A，然后获取符合条件的 Session 中访问了页面 3 又紧接着访问了页面 5 的次数为 B，
//那么 B/A 就是 3-5 的页面单跳转化率。
object 单跳转换率 {

}
