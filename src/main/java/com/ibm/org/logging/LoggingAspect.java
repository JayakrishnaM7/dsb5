package com.ibm.org.logging;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class LoggingAspect {
	private final Logger log = LoggerFactory.getLogger(this.getClass());

	@AfterReturning(pointcut = "execution(* com.ibm.org..*.*(..))", returning = "retVal")
	public void logAfterMethod(JoinPoint joinPoint, Object retVal) {
                if(retVal != null) {
                        log.info("returing {}" , retVal.getClass());
                        log.debug("-----------returning-- {}" , retVal);
                }
	}

	@Before("execution(* com.ibm.org..*.*(..))")
	public void logBeforeMethod(JoinPoint joinPoint) {
		log.info("***  Entering in Method : {} Of {}" ,joinPoint.getSignature().getName(),joinPoint.getTarget().getClass().getName());
	}

	@After("execution(* com.ibm.org..*.*(..))")
	public void logAfterMethod(JoinPoint joinPoint) {

		log.info("***{}  Exiting from  : {} Of {}" ,joinPoint.getSignature().getName(),joinPoint.getTarget().getClass().getName());
	}

	@AfterThrowing(pointcut = "execution(* com.ibm.org..*.*(..))", throwing = "e")
	public void logAfterThrowing(JoinPoint joinPoint, Throwable e) {
		log.error("An exception has been thrown in {}" , joinPoint.getSignature().getName() + "()");
		log.error("Cause : {}" , e.getCause());
	}

	@Around("execution(* com.ibm.org..*.*(..))")
	public Object profile(ProceedingJoinPoint pjp) throws Throwable { //NOSONAR
		try {
			long start = System.currentTimeMillis();

			Object output = pjp.proceed();

			Long elapsedTime = System.currentTimeMillis() - start;
			MDC.put("API", pjp.getSignature().toLongString());
			MDC.put("executionTime", String.valueOf(elapsedTime));
			log.info("Method execution time for API {} : {} milliseconds." , pjp.getSignature() ,elapsedTime);			
			return output;
		} finally {
			MDC.clear();
		}
	}
}
