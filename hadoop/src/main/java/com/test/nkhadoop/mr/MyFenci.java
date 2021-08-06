package com.test.nkhadoop.mr;

import org.ansj.app.keyword.KeyWordComputer;
import org.ansj.app.keyword.Keyword;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.*;
import org.ansj.util.MyStaticValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class MyFenci {
    public static void main(String[] args) {
//        String str = "送给男朋友，送给女朋友，送给别人，这花好丑" ;
//        for(Term s:NlpAnalysis.parse(str)){
//            if(s.getRealName().equals(" ")) continue;
//            System.out.println(s.getRealName());
//        }
        HashMap<String , Integer> test=new HashMap<>();
        test.put("女朋友",1);
        if(test.containsKey("男朋友")){
            test.put("男朋友",test.get("男朋友")+1);

        }
        else{
            test.put("男朋友",1);
        }
        System.out.println(test);
//        String tmpStr=BaseAnalysis.parse(" ").toString();
//        ArrayList<String> T=new ArrayList<>();
//        T.add(tmpStr);
//        T.add("");
//        System.out.println(T.toString());
//        if(tmpStr.equals(" "))
//            System.out.println("yes");
//
//        System.out.println(BaseAnalysis.parse(str));
//        System.out.println(ToAnalysis.parse(str));
//        System.out.println(DicAnalysis.parse(str));
//        System.out.println(IndexAnalysis.parse(str));
//        System.out.println(NlpAnalysis.parse(str));
//        KeyWordComputer kwc = new KeyWordComputer(10);
//        String title = "维基解密否认斯诺登接受委内瑞拉庇护";
//        String content = "有俄罗斯国会议员，9号在社交网站推特表示，美国中情局前雇员斯诺登，已经接受委内瑞拉的庇护，不过推文在发布几分钟后随即删除。俄罗斯当局拒绝发表评论，而一直协助斯诺登的维基解密否认他将投靠委内瑞拉。　　俄罗斯国会国际事务委员会主席普什科夫，在个人推特率先披露斯诺登已接受委内瑞拉的庇护建议，令外界以为斯诺登的动向终于有新进展。　　不过推文在几分钟内旋即被删除，普什科夫澄清他是看到俄罗斯国营电视台的新闻才这样说，而电视台已经作出否认，称普什科夫是误解了新闻内容。　　委内瑞拉驻莫斯科大使馆、俄罗斯总统府发言人、以及外交部都拒绝发表评论。而维基解密就否认斯诺登已正式接受委内瑞拉的庇护，说会在适当时间公布有关决定。　　斯诺登相信目前还在莫斯科谢列梅捷沃机场，已滞留两个多星期。他早前向约20个国家提交庇护申请，委内瑞拉、尼加拉瓜和玻利维亚，先后表示答应，不过斯诺登还没作出决定。　　而另一场外交风波，玻利维亚总统莫拉莱斯的专机上星期被欧洲多国以怀疑斯诺登在机上为由拒绝过境事件，涉事国家之一的西班牙突然转口风，外长马加略]号表示愿意就任何误解致歉，但强调当时当局没有关闭领空或不许专机降落。";
//        Collection<Keyword> result = kwc.computeArticleTfidf(title, content);
//        System.out.println(result);



    }
}
