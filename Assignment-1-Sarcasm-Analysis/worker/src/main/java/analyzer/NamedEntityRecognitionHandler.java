package analyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class NamedEntityRecognitionHandler {
    private final StanfordCoreNLP nerPipeline;
    private final List<String> entities = List.of("PERSON", "ORGANIZATION", "LOCATION");

    public NamedEntityRecognitionHandler() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize , ssplit , pos , lemma , ner");
        nerPipeline = new StanfordCoreNLP(props);
    }

    public List<String> findEntities(String review) {
        List<String> entitiesFound = new ArrayList<>();
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);
        // run all Annotators on this text
        nerPipeline.annotate(document);
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with
        // custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);

                if (entities.contains(ne)) {
                    entitiesFound.add(word + ":" + ne);
                }
            }
        }
        return entitiesFound;
    }

}
