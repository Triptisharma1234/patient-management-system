package com.pm.authservice.service;

import com.pm.authservice.model.User;
import com.pm.authservice.repository.UserRepositiory;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class UserService {

    private final UserRepositiory userRepository;

    public UserService(UserRepositiory userRepository){
        this.userRepository = userRepository;
    }

    public Optional<User> findByEmail(String email){
        return userRepository.findByEmail(email);
    }
}
