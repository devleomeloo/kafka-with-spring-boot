package com.kafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {

    @NotNull(message = "Book Id can't be null !")
    private Integer bookId;

    @NotBlank(message = "Book Name can't be blank !")
    private String bookName;

    @NotBlank(message = "Book Author can't be blank !")
    private String bookAuthor;
}
