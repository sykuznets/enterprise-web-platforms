package com.example.application.configuration;

import java.io.IOException;

import com.example.application.exception.UserUpsertApiException;
import com.example.application.service.CurrentEmployeeProvisioningService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NullMarked;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerExceptionResolver;

@RequiredArgsConstructor
public class CurrentEmployeeProvisioningFilter extends OncePerRequestFilter {

    private final CurrentEmployeeProvisioningService currentEmployeeProvisioningService;
    private final HandlerExceptionResolver handlerExceptionResolver;

    @Override
    @NullMarked
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        var authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null
                && authentication.getPrincipal() instanceof Jwt jwt) {
            try {
                currentEmployeeProvisioningService.ensureEmployeeExists(jwt);
            } catch (UserUpsertApiException ex) {
                handlerExceptionResolver.resolveException(request, response, null, ex);
                return;
            }
        }

        filterChain.doFilter(request, response);
    }
    
}
