package clustering;

import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Utils {

    public static Set<String> STOP_WORDS = new HashSet<>(Arrays.asList("a", "as", "able", "about",
            "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against",
            "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always",
            "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything",
            "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around",
            "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became",
            "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe",
            "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon",
            "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes",
            "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain",
            "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely",
            "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done",
            "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough",
            "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything",
            "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five",
            "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further",
            "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten",
            "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes",
            "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers",
            "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id",
            "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate",
            "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd",
            "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately",
            "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little",
            "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely",
            "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd",
            "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine",
            "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere",
            "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto",
            "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over",
            "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible",
            "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably",
            "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say",
            "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen",
            "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should",
            "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime",
            "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub",
            "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx",
            "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there",
            "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd",
            "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though",
            "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards",
            "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless",
            "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually",
            "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well",
            "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence",
            "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever",
            "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will",
            "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet",
            "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"));

    public static String listToString(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i ++) {
            sb.append(list.get(i));
            if (i != list.size() - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    public static <E> Map<E, Integer> enumerate(List<E> list) {
        Map<E, Integer> enumerated = new HashMap<>();
        int count = 0;
        for (E s : list) {
            enumerated.put(s, count ++);
        }
        return enumerated;
    }

    public static <S, T> Map<S, T> getMap(List<Tuple2<S, T>> tuples) {
        Map<S, T> map = new HashMap<>();
        for (Tuple2<S, T> t : tuples) {
            map.put(t._1(), t._2());
        }
        return map;
    }
}
