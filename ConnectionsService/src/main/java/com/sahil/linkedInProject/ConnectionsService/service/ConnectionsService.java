package com.sahil.linkedInProject.ConnectionsService.service;

import com.sahil.linkedInProject.ConnectionsService.auth.AuthContextHolder;
import com.sahil.linkedInProject.ConnectionsService.entity.Person;
import com.sahil.linkedInProject.ConnectionsService.repository.PersonRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class ConnectionsService {

    private final PersonRepository personRepository;

    public List<Person> getFirstDegreeConnectionsOfUser(Long userId) {
        log.info("Getting first degree connections of user with ID: {}", userId);

        return personRepository.getFirstDegreeConnections(userId);
    }

    public void sendConnectionRequest(Long receiverId){
        Long senderId = AuthContextHolder.getCurrentUserId();

        if(senderId.equals(receiverId)){
            throw new RuntimeException("Both sender and receiver are the same");
        }

        boolean alreadySentRequest  = personRepository.connectionRequestExists(senderId,receiverId);
        if(alreadySentRequest){
            throw new RuntimeException("Connection request already exists, cannot send again");
        }

        boolean alreadyConnected = personRepository.alreadyConnected(senderId,receiverId);
        if(alreadyConnected){
            throw new RuntimeException("Connection Already exists, cannot add new connect request");
        }

        log.info("Successfully sent the connection request");
        personRepository.addConnectionRequest(senderId,receiverId);

    }

    public void acceptConnectionRequest(Long senderId){

        Long receiverId = AuthContextHolder.getCurrentUserId();

        if(senderId.equals(receiverId)){
            throw new RuntimeException("Both sender and receiver are the same");
        }
        boolean alreadyConnected = personRepository.alreadyConnected(senderId,receiverId);
        if(alreadyConnected){
            throw new RuntimeException("Connection Already exists, cannot add new connect request");
        }

        boolean alreadySentRequest  = personRepository.connectionRequestExists(senderId,receiverId);
        if(!alreadySentRequest){
            throw new RuntimeException("No Connection request exists, cannot accept without request");
        }
        personRepository.acceptConnectionRequest(senderId,receiverId);
        log.info("Successfully accepted the connection request with senderId: {}, receiverId: {}",senderId,receiverId);
    }

    public void rejectConnectionRequest(Long senderId){

        Long receiverId = AuthContextHolder.getCurrentUserId();

        if(senderId.equals(receiverId)){
            throw new RuntimeException("Both sender and receiver are the same");
        }

        boolean alreadySentRequest  = personRepository.connectionRequestExists(senderId,receiverId);
        if(!alreadySentRequest){
            throw new RuntimeException("No Connection request exists, cannot accept without request");
        }
        personRepository.rejectConnectionRequest(senderId,receiverId);
        log.info("Rejected a connection request with senderId: {}, receiverId: {}",senderId,receiverId);
    }

}
