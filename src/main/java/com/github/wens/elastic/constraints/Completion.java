package com.github.wens.elastic.constraints;

import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

public class Completion {
    public static String SUGGESTION_NAME = "completion";
    private static String field;
    private static String value;
    private static int size;

    public Completion(String field, String value, int size) {
        Completion.field = field;
        Completion.value = value;
        Completion.size = size;
    }

    public static CompletionSuggestionBuilder createBuilder() {
        CompletionSuggestionBuilder suggestionsBuilder = new CompletionSuggestionBuilder(SUGGESTION_NAME);
        suggestionsBuilder.text(value);
        suggestionsBuilder.field(field);
        suggestionsBuilder.size(size);
        return suggestionsBuilder;
    }

    public static SuggestResponse buildSuggest(String index, Client client) {
        return client.prepareSuggest(index).addSuggestion(createBuilder()).execute().actionGet();
    }
}
