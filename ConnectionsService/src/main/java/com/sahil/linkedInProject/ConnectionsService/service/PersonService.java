package com.sahil.linkedInProject.ConnectionsService.service;

import com.sahil.linkedInProject.ConnectionsService.entity.Person;
import com.sahil.linkedInProject.ConnectionsService.repository.PersonRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class PersonService {

    private final PersonRepository personRepository;

    public Person createPerson(Long userId, String name){
        Person person = Person
                .builder()
                .name(name)
                .userId(userId)
                .build();

        return personRepository.save(person);

    }


}
