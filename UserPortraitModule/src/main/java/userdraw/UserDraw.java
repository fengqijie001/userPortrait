package userdraw;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Create by fengqijie
 * 2019/2/27 10:18
 *
 * 用户画像
 */
@Data
public class UserDraw {

    private String startTimeDay;
    private String MDN;
    private double male;
    private double female;
    private double age1;
    private double age2;
    private double age3;
    private double age4;
    private double age5;

    // 重写toString
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(startTimeDay).append("|");
        sb.append(MDN).append("|");
        sb.append(new BigDecimal(male).setScale(3, 4).doubleValue()).append("|");
        sb.append(new BigDecimal(female).setScale(3, 4).doubleValue()).append("|");
        sb.append(new BigDecimal(age1).setScale(3, 4).doubleValue()).append("|");
        sb.append(new BigDecimal(age2).setScale(3, 4).doubleValue()).append("|");
        sb.append(new BigDecimal(age3).setScale(3, 4).doubleValue()).append("|");
        sb.append(new BigDecimal(age4).setScale(3, 4).doubleValue()).append("|");
        sb.append(new BigDecimal(age5).setScale(3, 4).doubleValue()).append("|");

        return sb.toString();
    }

    // 对男女性别的计算
    public void protraitSex(double male2, double female2, long times) {
        double sum = this.male + this.female + (male2 + female2) * times;
        if (sum != 0) {
            this.male = (this.male + male2 * times) / sum;
            this.female = (this.female + female2 * times) / sum;
        }
    }

    // 对年龄段的计算
    public void protraitAge(double pAge1, double pAge2, double pAge3, double pAge4, double pAge5, long times) {
        double sum = (age1 + age2 + age3 + age4 + age5 ) // 之前的APP的
                + (pAge1 + pAge2 + pAge3 + pAge4 + pAge5 ) * times;// 当前的APP的
        if (sum != 0){
            this.age1 = (pAge1 * times + age1) / sum;
            this.age2 = (pAge2 * times + age2) / sum;
            this.age3 = (pAge3 * times + age3) / sum;
            this.age4 = (pAge4 * times + age4) / sum;
            this.age5 = (pAge5 * times + age5) / sum;
        }
    }

    // 初始化男女概率
    public void initSex(float male, float female) {
        float sum = male + female;
        if (sum != 0) {
           this.male = male / sum;
           this.female = female / sum;
        }
    }

    // 初始化年龄段概率
    public void initAge(float pAge1, float pAge2, float pAge3, float pAge4, float pAge5) {
        float sum = pAge1 + pAge2 + pAge3 + pAge4 + pAge5;
        if (sum != 0){
            this.age1 = pAge1 / sum;
            this.age2 = pAge2 / sum;
            this.age3 = pAge3 / sum;
            this.age4 = pAge4 / sum;
            this.age5 = pAge5 / sum;
        }
    }


}
